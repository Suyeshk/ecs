# Import the necessary libraries
import pandas as pd
import boto3
from io import StringIO

# Create an S3 client
s3 = boto3.client('s3')

# Set the source and destination bucket names
src_bucket_name = 'bucketsnowflakes47'
dst_bucket_name = 'snowoutput'

# Set the source and destination keys (file names)
src_key = 'csv/data.csv'
dst_key = 'csv/data2.csv'

# Use the S3 client to download the object
response = s3.get_object(Bucket=src_bucket_name, Key=src_key)
data = response['Body'].read()

# Convert the downloaded bytes to a DataFrame
df = pd.read_csv(StringIO(data.decode()))

# Add a new column to the DataFrame
df['new_column'] = 'default_value'

# Use the S3 client to upload the object
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)
s3.put_object(Bucket=dst_bucket_name, Key=dst_key, Body=csv_buffer.getvalue())
