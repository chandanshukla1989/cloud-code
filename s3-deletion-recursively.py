import os
import boto3
import pandas as pd

#initiate s3 resource
s3 = boto3.resource('s3')

# select bucket
my_bucket = s3.Bucket('employee1')
client = boto3.client('s3',aws_access_key_id='AKIAZSPQIDERL3OO25F4',aws_secret_access_key='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')
#client = boto3.client('s3') #low-level functional API

# download file into current directory
for s3_object in my_bucket.objects.all():
    #print(s3_object)
    #print(s3_object.key)
    if "csv" in s3_object.key:
        print(s3_object.key)
        obj = client.get_object(Bucket='employee1', Key=s3_object.key)
        grid_sizes = pd.read_csv(obj['Body'])
        grid_sizes
        total_rows = grid_sizes.shape[0]
        if total_rows==2:
            #s3HTTP(verb = "DELETE", bucket = "employee1",path = s3_object.key, parse_response = FALSE,key = 'AKIAZSPQIDERL3OO25F4', secret = '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')
            s3.Object('employee1',s3_object.key).delete()
        total_rows
        print(total_rows)

        
    
    # Need to split s3_object.key into path and file name, else it will give error file not found.
