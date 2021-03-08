import boto3
import csv
#Enter your own aws keys 
s3 = boto3.resource('s3', aws_access_key_id = '', aws_secret_access_key='')
dyndb = boto3.resource('dynamodb',aws_access_key_id = '', aws_secret_access_key='', region_name='us-west-2')
try:
	s3.create_bucket(Bucket='datacont-kevitsui', CreateBucketConfiguration={'LocationConstraint': 'us-west-2'})
	table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'Partitionkey',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'RowKey',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Partitionkey',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'RowKey',
                'AttributeType': 'S'
            },

        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
except Exception:
	pass

# table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
table = dyndb.Table("DataTable")
urlbase = "https://datacont-kevitsui.s3-us-west-2.amazonaws.com/"
with open('experiments.csv', 'r') as csvfile:
	csvf = csv.reader(csvfile)
	next(csvf)
	for item in csvf:
		body = open(item[4], 'rb')
		s3.Object('datacont-kevitsui', item[4]).put(Body=body)
		md = s3.Object('datacont-kevitsui', item[4]).Acl().put(ACL='public-read')
		url = urlbase + item[4]
		metadata_item = {'Partitionkey': item[0], 'RowKey': item[1], 'description': item[3], 'date':item[2], 'url':url}
		table.put_item(Item=metadata_item)
		