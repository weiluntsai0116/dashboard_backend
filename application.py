from flask import Flask, request, Response
import pandas as pd
import re
import boto3
import apps.db_access as db_access
import apps.security as security
from io import StringIO
import json

application = Flask(__name__)
app = application


# Get each field from request
def log_and_extract_input(path_params=None):
    path = request.path
    args = dict(request.args)
    headers = dict(request.headers)
    method = request.method
    try:
        if request.data is not None:
            data = request.json
        else:
            data = None
    except Exception as e:
        # This would fail the request in a more real solution.
        # logger.error("You sent something but I could not get JSON out of it.")
        data = ""

    inputs = {
        "path": path,
        "method": method,
        "path_params": path_params,
        "query_params": args,
        "headers": headers,
        "body": data
    }
    # print(inputs)
    return inputs


# Create error response by error message string and its status code
def create_error_res(error_msg, code):
    return Response(json.dumps({"message": error_msg}),
                    status=code, content_type="application/json")


# Create successful response by its json payload and status code
def create_res(json_msg, code):
    return Response(json.dumps(json_msg, default=str),
                    status=code, content_type="application/json")


# create (post): user_id, signal_id, signal_description, s3

# Endpoint to create a new alert
@app.route('/api/signal/create', methods=['POST'])
def create_signal():
    inputs = log_and_extract_input()
    info = inputs["body"]
    (user_id, signal_id, signal_description, s3) = (
    info["user_id"], info["signal_id"], info["signal_description"], info["s3"])
    print(info)

    if s3 is None:
        create = u'''Create: Fail! Lack of S3 filename'''
    elif '.csv' not in s3:
        create = u'''Create: Fail! S3 filename format should be *.csv'''
    else:
        signal_id_list = db_access.get_all_signal_id()
        signal_id = db_access.first_missing_positive(signal_id_list)
        if user_id == "" or signal_id == "":
            create = u'''Create: Fail! Lack of Signal ID'''
        elif type(signal_id) != int:
            create = u'''Create: Fail! Invalid signal ID'''
        elif db_access.is_signal_exist(user_id, signal_id):
            create = u'''Create: Fail! (User ID, Signal ID) is 'duplicate'''
        else:
            try:
                client = boto3.client('s3', aws_access_key_id=security.aws_id,
                                      aws_secret_access_key=security.aws_secret)

                csv_obj = client.get_object(Bucket='user-signal-data', Key=s3)
                db_access.insert_signal(user_id, signal_id, signal_description, s3)
                create = 'Create: Pass!'
            except Exception as e:
                create = u'''Create: Fail! S3 file \"{}\" is not exist: {}'''.format(s3, e)
    return create_res({"message": create}, 200)


@app.route('/api/signal/modify', methods=['PUT'])
def modify_signal():
    inputs = log_and_extract_input()
    info = inputs["body"]
    (user_id, signal_id, signal_description, s3) = (
    info["user_id"], info["signal_id"], info["signal_description"], info["s3"])
    print(info)

    if user_id == "" or signal_id == "":
        modify = u'''Modify: Fail! Lack of User ID, Signal ID'''
    elif s3 is not None and s3 != "" and ('.csv' not in s3):
        modify = u'''Create: Fail! S3 filename format should be *.csv'''
    elif not re.search('^\d*$', signal_id):
        modify = u'''Modify: Fail! Invalid signal ID'''
    elif not db_access.is_signal_exist(user_id, signal_id):
        modify = u'''Modify: Fail! (User ID, Signal ID) is not exist'''
    else:
        if s3 is not None and s3 != "":
            try:
                client = boto3.client('s3', aws_access_key_id=security.aws_id,
                                      aws_secret_access_key=security.aws_secret)

                csv_obj = client.get_object(Bucket='user-signal-data', Key=s3)
                db_access.update_signal(user_id, signal_id, signal_description, s3)
                modify = 'Modify: Pass!'  # u'''Modify: {} times'''.format(modify_n_clicks)
            except Exception as e:
                modify = u'''Create: Fail! S3 file \"{}\" is not exist: {}'''.format(s3, e)
        else:
            db_access.update_signal(user_id, signal_id, signal_description, s3)
            modify = 'Modify: Pass!'  # u'''Modify: {} times'''.format(modify_n_clicks)

    return create_res({"message": modify}, 200)


@app.route('/api/signal/read', methods=['GET'])
def read_signal():
    inputs = log_and_extract_input()
    info = inputs["body"]
    (user_id, signal_id) = (info["user_id"], info["signal_id"])
    print(info)

    s3_df, cols = None, None
    if user_id == "" or signal_id == "":
        read = u'''Read: Fail! Lack of User ID or Signal ID'''
    elif not re.search('^\d*$', signal_id):
        read = u'''Create: Fail! Invalid signal ID'''
    elif not db_access.is_signal_exist(user_id, signal_id):
        read = u'''Read: Fail! (User ID, Signal ID) is not exist'''
    else:
        s3_filename = db_access.read_signal(user_id, signal_id)
        print("user_id = ", user_id, "signal_id = ", signal_id)
        print("s3_filename = ", s3_filename)

        client = boto3.client('s3', aws_access_key_id=security.aws_id,
                              aws_secret_access_key=security.aws_secret)

        csv_obj = client.get_object(Bucket='user-signal-data', Key=s3_filename)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')
        s3_df = pd.read_csv(StringIO(csv_string))

        # for local test :
        # s3_df = pd.read_csv(s3_filename)

        print("s3_df = ", s3_df)
        cols = list(s3_df.columns)
        print(cols)

        if len(cols) < 2:
            read = "Data has less than 2 columns "
        else:
            read = u'''Read: Pass!'''

    return create_res({"message": read, "csv_string": csv_string}, 200)


@app.route('/api/signal/modify', methods=['DELETE'])
def delete_signal():
    inputs = log_and_extract_input()
    info = inputs["body"]
    (user_id, signal_id, signal_description) = (
    info["user_id"], info["signal_id"], info["signal_description"])
    print(info)

    if user_id == "" or signal_id == "":
        delete = u'''Delete: Fail! Lack of User ID or Signal ID'''
    elif not re.search('^\d*$', signal_id):
        delete = u'''Create: Fail! Invalid signal ID'''
    elif not db_access.is_signal_exist(user_id, signal_id):
        delete = u'''Delete: Fail! (User ID, Signal ID) is not exist'''
    else:
        s3_filename = db_access.read_signal(user_id, signal_id)
        db_access.delete_signal(user_id, signal_id)
        myresult = db_access.is_csv_needed(s3_filename)
        if not myresult:
            s3_rsc = boto3.resource(u's3')
            bucket = s3_rsc.Bucket(u'user-signal-data')
            bucket.Object(key=s3_filename).delete()
        # print("myresult =", myresult)
        delete = u'''Delete: Pass!'''

    return create_res({"message": delete}, 200)


if __name__ == '__main__':
    application.run(debug=True, port=8080)
