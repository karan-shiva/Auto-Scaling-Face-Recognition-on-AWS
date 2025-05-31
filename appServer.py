import boto3
import os
import subprocess

# AWS credentials
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY_ID")
REGION = os.environ('REGION')

# SQS and S3 configurations
REQ_QUEUE_URL = os.environ.get('REQ_QUEUE_URL')
RESP_QUEUE_URL = os.environ.get('RESP_QUEUE_URL')
S3_INPUT_BUCKET = os.environ.get('S3_INPUT_BUCKET')
S3_OUTPUT_BUCKET = os.environ.get('S3_OUTPUT_BUCKET')
LOCAL_IMAGE_DIR = os.environ.get('LOCAL_IMAGE_DIR')

# Initialize AWS clients
sqs_client = boto3.client('sqs', region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

s3_client = boto3.client('s3', region_name=REGION,
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY)

def download_file_from_s3(file_name):
    try:
        local_file_path = os.path.join(LOCAL_IMAGE_DIR, file_name)
        s3_client.download_file(S3_INPUT_BUCKET, file_name, local_file_path)
        print(f"Downloaded {file_name} from S3")
        return local_file_path
    except Exception as e:
        print(f"Error downloading file from S3: {str(e)}")
        return None

def run_ml_model(file_path):
    try:
        # Call the face recognition model with the input file
        result = subprocess.check_output(['python3', '/home/ubuntu/model/face_recognition.py', file_path])
        classification = result.decode('utf-8').strip()
        print(f"Model output for {file_path}: {classification}")
        return classification
    except subprocess.CalledProcessError as e:
        print(f"Error running ML model: {e.output.decode()}")
        return None

def upload_classification_to_s3(file_name, classification):
    try:
        s3_client.put_object(
            Bucket=S3_OUTPUT_BUCKET,
            Key=file_name,
            Body=classification
        )
        print(f"Uploaded classification result for {file_name} to S3 bucket {S3_OUTPUT_BUCKET}")
    except Exception as e:
        print(f"Error uploading classification to S3: {str(e)}")

def send_response_to_sqs(file_name, classification):
    try:
        message = f"{file_name}:{classification}"
        sqs_client.send_message(
            QueueUrl=RESP_QUEUE_URL,
            MessageBody=message
        )
        print(f"Sent response to SQS: {message}")
    except Exception as e:
        print(f"Error sending response to SQS: {str(e)}")

def process_message(file_name):
    # Step 1: Download the file from S3
    file_path = download_file_from_s3(file_name)
    if file_path is None:
        return

    # Step 2: Run the ML model on the file
    classification = run_ml_model(file_path)
    if classification is None:
        return

    # Step 3: Upload the classification result to S3
    upload_classification_to_s3(file_name, classification)

    # Step 4: Send response message to SQS
    send_response_to_sqs(file_name, classification)

def poll_sqs():
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=REQ_QUEUE_URL,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10  # Adjust as necessary
            )
            messages = response.get('Messages', [])
            if not messages:
                print("No messages in queue.")
                continue

            message = messages[0]
            file_name = message['Body']
            receipt_handle = message['ReceiptHandle']

            print(f"Processing file: {file_name}")

            # Process the message (download, run model, upload result, send SQS response)
            process_message(file_name)

            # Delete the processed message from the queue
            sqs_client.delete_message(
                QueueUrl=REQ_QUEUE_URL,
                ReceiptHandle=receipt_handle
            )
            print(f"Deleted message from SQS: {file_name}")

        except Exception as e:
            print(f"Error processing message: {str(e)}")

if __name__ == '__main__':
    print("Starting app tier to process messages from SQS...")
    poll_sqs()
