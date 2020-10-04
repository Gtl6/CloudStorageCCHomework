import boto3

s3 = boto3.resource('s3',
    aws_access_key_id='AKIAXTBH3X7S7N5BAN3C',
    aws_secret_access_key='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' )
    
try:
    s3.create_bucket(Bucket='datacont-griffin-homework', CreateBucketConfiguration={
    'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

dyndb = boto3.resource('dynamodb', 
    region_name='us-west-2',
    aws_access_key_id='AKIAXTBH3X7S7N5BAN3C',
    aws_secret_access_key='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' )
    
try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[{
            'AttributeName': 'PartitionKey',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'RowKey',
            'KeyType': 'RANGE'
        }],
        AttributeDefinitions=[
        {
            'AttributeName': 'PartitionKey',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'RowKey',
            'AttributeType': 'S'
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        })
except:
        table = dyndb.Table("DataTable")
            
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
print(table.item_count)

import csv
with open('./experiments.csv') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open('./'+item[3], 'rb')
        s3.Object('datacont-griffin-homework', item[3]).put(Body=body)
        md = s3.Object('datacont-griffin-homework', item[3]).Acl().put(ACL='public-read')
        
        url = " https://s3-us-west-2.amazonaws.com/datacont-griffin-homework/"+item[3]
        metadata_item = {'PartitionKey': item[0], 'RowKey': item[1],
            'description' : item[4], 'date' : item[2], 'url':url}
            
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may be there or another failure")
            
        print("Finished the line")
        
        
response = table.get_item(
    Key={
        'PartitionKey': '1',
        'RowKey': '1'
        }
    )
item = response['Item']
print(item)
        
        
        
        
