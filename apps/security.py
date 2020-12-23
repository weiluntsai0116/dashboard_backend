import os

login_url = "http://user-service-dash.eba-y82cxuwr.us-east-2.elasticbeanstalk.com/"
user_url = "http://user-service-dash.eba-y82cxuwr.us-east-2.elasticbeanstalk.com/users/"
catalog_url = "http://signaldevv20-env.eba-2ibxmk54.us-east-2.elasticbeanstalk.com/"
alert_url = "http://ec2-13-58-238-7.us-east-2.compute.amazonaws.com:5000/"

aws_id = os.environ['AWS_ID']
aws_secret = os.environ['AWS_SECRET']

jwt_secret = os.environ['JWT_SECRET']
jwt_algo = os.environ['JWT_ALGO']
jwt_exp_delta_sec = float(os.environ['JWT_EXP'])

fernet_secret = os.environ['TOKEN_SECRET'].encode('utf-8')