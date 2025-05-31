import csv
from flask import Flask, request, jsonify
import os
import boto3

# Initialize AWS clients
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
REGION = os.environ.get('REGION')

sqs_client = boto3.client('sqs', region_name=REGION,
                          aws_access_key_id=AWS_ACCESS_KEY,
                          aws_secret_access_key=AWS_SECRET_KEY)

s3_client = boto3.client('s3', region_name=REGION,
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY)

REQ_QUEUE_URL = os.environ.get('REQ_QUEUE_URL')
RESP_QUEUE_URL = os.environ.get('RESP_QUEUE_URL')
S3_BUCKET_NAME = os.environ.get('S3_INPUT_BUCKET')
IMAGE_DIR = '/home/ubuntu/face_images_1000'

app = Flask(__name__)

@app.route('/', methods=['POST'])
def handle_post_request():
    if 'inputFile' not in request.files:
        return "No file part", 400

    file = request.files['inputFile']
    filename = file.filename

    print(f"File name: {filename}")

    # Upload file to S3
    file_path = os.path.join(IMAGE_DIR, filename)
    try:
        with open(file_path, 'rb') as f:
            # s3_client.upload_fileobj(f, S3_BUCKET_NAME, filename)
            s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=filename, Body=f)
        print(f"Uploaded {filename} to S3 bucket {S3_BUCKET_NAME}")
    except Exception as e:
        return f"Error uploading to S3: {str(e)}", 501

    # Send message to SQS queue
    try:
        sqs_client.send_message(
            QueueUrl=REQ_QUEUE_URL,
            MessageBody=filename
        )
        print(f"Sent message to SQS queue {REQ_QUEUE_URL} with filename {filename}")
    except Exception as e:
        return f"Error sending SQS message: {str(e)}", 502

    # Poll SQS for response from "1229598320-resp-queue"
    while True:
        try:
            response = sqs_client.receive_message(
                QueueUrl=RESP_QUEUE_URL,
                MessageAttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10  # Adjust wait time as needed
            )
            messages = response.get('Messages', [])
            
            for i in range(len(messages)):
                message = messages[i]
                message_body = message['Body']
                receipt_handle = message['ReceiptHandle']

                # Parse response and check if it matches the filename
                file_response, classification = message_body.split(':')
                if file_response == filename:
                    print(f"Matching response found: {message_body}")
                    
                    # Delete message from queue after processing
                    sqs_client.delete_message(
                        QueueUrl=RESP_QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    print(f'Deleted message ID: {message["MessageId"]}')
                    return f"{filename}:{classification}", 200

        except Exception as e:
            return f"Error receiving message from SQS: {str(e)}", 504

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
