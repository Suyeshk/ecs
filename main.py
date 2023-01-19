
import pandas as pd
import boto3
import s3fs
from io import StringIO

#define global variable
queue_url = 'https://sqs.us-east-1.amazonaws.com/897708493501/etl'

#create clients
sqs = boto3.client('sqs')
s3 = boto3.resource('s3')

if __name__ == "__main__":
    #Collecting queue message one by one until queue is empty
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )
        
        #condition to check if queue is empty   
        try:
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            bucket = response['Messages'][0]['Body']
            key = response['Messages'][0]['MessageAttributes']['key']['StringValue']
              

            #read dataframe
            path = 's3://bucketsnowflakes47/csv/'
            df = pd.read_csv(path, header=0, sep=",")
            
            #process the dataframe
            shuffled_df = df.sample(frac=1)
            
            #write s3/data
            folder = 'processed'
            data_key = folder + '/data.csv'
            csv_buffer = StringIO()
            training_df.to_csv(csv_buffer)
            s3.Object(bucket, data_key).put(Body=csv_buffer.getvalue())
            

            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
                )

                
        #manage case queue is empty
        except KeyError:
            print('no messages anymore')
            break
