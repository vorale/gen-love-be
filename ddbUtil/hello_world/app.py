import boto3
import json
import os
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('DDB_TN')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    operation = event['operation']
    
    if operation == 'insert':
        return insert_item(event['item'])
    elif operation == 'update':
        return update_item(event['key'], event['updateExpression'], event['expressionAttributeValues'])
    elif operation == 'delete':
        return delete_item(event['key'])
    elif operation == 'query':
        return query_items('', '')
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Unsupported operation')
        }

def insert_item(item):
    try:
        response = table.put_item(Item=item)
        return {
            'statusCode': 200,
            'body': json.dumps('Item inserted successfully')
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(e.response['Error']['Message'])
        }

def update_item(key, update_expression, expression_attribute_values):
    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues="UPDATED_NEW"
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Item updated successfully')
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(e.response['Error']['Message'])
        }

def delete_item(key):
    try:
        response = table.delete_item(Key=key)
        return {
            'statusCode': 200,
            'body': json.dumps('Item deleted successfully')
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(e.response['Error']['Message'])
        }

def query_items(key_condition_expression, filter_expression=None):
    try:
        response = table.scan()
        return {
            'statusCode': 200,
            'body': json.dumps(response['Items'])
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps(e.response['Error']['Message'])
        }