import boto3
import time
import os

# Initialize AWS clients
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY_ID")
REGION = os.environ('REGION')
AMITEMPLATE = "ami-0dcaa404c9a51a92a"

ec2_client = boto3.client('ec2', region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

sqs_client = boto3.client('sqs', region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

REQ_QUEUE_URL = os.environ.get('REQ_QUEUE_URL')
RESP_QUEUE_URL = os.environ.get('RESP_QUEUE_URL')

app_teir_count = 0

def create_app_tier(msg_count, app_count):
    print(f"Number of messages in create_app_tier: {msg_count}")
    print(f"Number of apps in create_app_tier: {app_count}")
    for i in range(app_count, msg_count):
        ec2_client.run_instances(
            ImageId=AMITEMPLATE,
            InstanceType='t2.micro',
            MinCount=1,
            MaxCount=1,
            KeyName="KaranEast",
            SecurityGroupIds=["sg-0448041972c421292"],
            TagSpecifications=[{
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': f'app-tier-instance-{i+1}'}]
            }]
        )
        print(f"Created instace app-tier-instance-{i+1}")

def delete_all_messages():
    while True:
        # Receive messages from the queue
        response = sqs_client.receive_message(
            QueueUrl=RESP_QUEUE_URL,
            MaxNumberOfMessages=10,  # Maximum number of messages to retrieve (1-10)
            WaitTimeSeconds=1        # Long polling
        )

        # Check if there are any messages
        messages = response.get('Messages', [])
        if not messages:
            break  # Exit the loop if there are no more messages

        # Delete the received messages
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            sqs_client.delete_message(
                QueueUrl=RESP_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f'Deleted message ID: {message["MessageId"]}')

# Terminate instances with the name pattern 'app-tier-instance-<instance#>'
def terminate_app_tier_instances():
    response = ec2_client.describe_instances(
        Filters=[
            {
                'Name': 'tag:Name',
                'Values': [f'app-tier-instance-{i}' for i in range(1, 21)]
            },
            {
                'Name': 'instance-state-name',
                'Values': ['running']
            }
        ]
    )

    instance_ids = []
    for reservation in response['Reservations']:
        for instance in reservation['Instances']:
            instance_ids.append(instance['InstanceId'])
    
    if instance_ids:
        ec2_client.terminate_instances(InstanceIds=instance_ids)
        print(f"Terminating instances: {instance_ids}")

start = 0
end = 0

while True:
    req_res = sqs_client.get_queue_attributes(
        QueueUrl=REQ_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )

    resp_res = sqs_client.get_queue_attributes(
        QueueUrl=RESP_QUEUE_URL,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )

    req_msgs = int(req_res['Attributes'].get('ApproximateNumberOfMessages', 0)) + int(req_res['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
    resp_msgs = int(resp_res['Attributes'].get('ApproximateNumberOfMessages', 0)) + int(resp_res['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))

    # if(req_msgs + resp_msgs > 0):
    #     print(f"Number of message in Req: {req_msgs}")
    #     print(f"Number of message in Resp: {resp_msgs}")
    req_msgs = 20 if req_msgs > 20 else req_msgs
    if(req_msgs > 0):
        if(req_msgs > app_teir_count):
            create_app_tier(req_msgs, app_teir_count)
            app_teir_count = req_msgs
        start = time.time()

    if(req_msgs == 0 and app_teir_count > 0):
        end = time.time()
        print(f"Time diff = {end-start}")
        if end-start > 20:
            terminate_app_tier_instances()
            app_teir_count = 0
            delete_all_messages()