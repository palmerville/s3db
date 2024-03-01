import boto3
import json
from botocore.exceptions import ClientError

class Collection:
    def __init__(self, db, collection_name):
        self.db = db
        self.collection_name = collection_name

    def insert(self, index, record):
        self.db.insert(self.collection_name, index, record)

    def select(self, index):
        return self.db.select(self.collection_name, index)

    def fetchall(self):
        return self.db.fetchall(self.collection_name)

    def count(self):
        return self.db.count(self.collection_name)

    def delete(self, index):
        self.db.delete(self.collection_name, index)

    def find(self, query):
        return self.db.find(self.collection_name, query)


class S3db:
    s3 = boto3.client('s3')

    def __init__(self, bucket):
        self.bucket = bucket

    def __getattr__(self, collection_name):
        return Collection(self, collection_name)

    def insert(self, collection, index, record):
        s3_object_content = json.dumps(record)
        self.s3.put_object(Bucket=self.bucket, Key=f'{collection}/{index}', Body=s3_object_content)

    def select(self, collection, index):
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=f'{collection}/{index}')
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            return data
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise

    def fetchall(self, collection):
        records = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f'{collection}/'):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    try:
                        response = self.s3.get_object(Bucket=self.bucket, Key=key)
                        content = response['Body'].read().decode('utf-8')
                        data = json.loads(content)
                        records.append(data)
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'NoSuchKey':
                            raise
        return records            

    def count(self, collection):
        count = 0
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f'{collection}/'):
            if 'Contents' in page:
                count += len(page['Contents'])
        return count
    

    def delete(self, collection, index):
        self.s3.delete_object(Bucket=self.bucket, Key=f'{collection}/{index}')


    def find(self, collection, query):
        results = []
        paginator = self.s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f'{collection}/'):
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Construct the SQL query to search for the JSON fields
                    conditions = []
                    for field_name, field_value in query.items():
                        # Escape single quotes in the field value if it's a string
                        if isinstance(field_value, str):
                            field_value = field_value.replace("'", "''")
                            condition = f"s.\"{field_name}\" = '{field_value}'"
                        else:
                            condition = f"s.\"{field_name}\" = {field_value}"
                        conditions.append(condition)
                    sql_expression = "SELECT * FROM s3object s WHERE " + " AND ".join(conditions)
                    try:
                        response = self.s3.select_object_content(
                            Bucket=self.bucket,
                            Key=key,
                            ExpressionType='SQL',
                            Expression=sql_expression,
                            InputSerialization={'JSON': {'Type': 'DOCUMENT'}},
                            OutputSerialization={'JSON': {}}
                        )
                        # Process the response records
                        for event in response['Payload']:
                            if 'Records' in event:
                                record = event['Records']['Payload'].decode('utf-8')
                                data = json.loads(record)
                                results.append(data)
                    except Exception as e:
                        print(f"Error processing object {key}: {e}")
        return results        

