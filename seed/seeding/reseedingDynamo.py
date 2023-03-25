import random
import boto3
from botocore.exceptions import ClientError
import botocore
import json
from decimal import *
from config import PRODUCTS_TABLE_NAME, PROVIDERS_NAMES



config_boto = botocore.config.Config(
    read_timeout=900,
    connect_timeout=900,
    retries={"max_attempts": 0}
)

ddb = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000',
    region_name='us-east-1',
    config=config_boto
)

def create_dynamo_table(logger):
    logger.info('Creating ProductsTable into DynamoDB')
    ddb.create_table(
        TableName=PRODUCTS_TABLE_NAME,
        AttributeDefinitions=[
            {
                "AttributeName": "itemCode",
                "AttributeType": "S"
            },
            {
                "AttributeName": "itemDescription",
                "AttributeType": "S"
            },
            {
                "AttributeName": "itemProvider",
                "AttributeType": "S"
            },
            
        ],
        GlobalSecondaryIndexes=[
        {
            'IndexName': 'idx1',
            'KeySchema': [
               {
                  'AttributeName': 'itemProvider',
                  'KeyType': 'HASH'
               }
             ],
             'Projection': {
               'ProjectionType': 'ALL'
             },
             'ProvisionedThroughput': {
                  'ReadCapacityUnits': 1,
                  'WriteCapacityUnits': 1
             }
        }
    ],
        KeySchema=[
            {
                "AttributeName": "itemCode",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "itemProvider",
                "KeyType": "RANGE"
            }
        ],ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

def _already_seeded(logger):
    logger.info('Check if data is already seeded')
    is_seeded = False
    try:
        table = ddb.Table(PRODUCTS_TABLE_NAME)
        logger.info(table)
        is_seeded = True
        # scan_response = table.scan(TableName=PRODUCTS_TABLE_NAME)
        # items = scan_response['Items']
        # if len(items):
        #     is_seeded = True
    except ClientError as error:
        if error.response['Error']['Code'] == 'ResourceNotFoundException':
            pass
        else:
            logger.error(f"============= DYNAMODB ERROR {e.response['Error']['Code']} ================")
    return is_seeded

def populate_dynamo_table(logger):
    logger.info('Populating ProductsTable into DynamoDB')
    table = ddb.Table(PRODUCTS_TABLE_NAME)
    array_of_items = []
    for provider in PROVIDERS_NAMES:
        logger.info(f'Creating info for provider: {provider}')
        path_to_json = f'../../data/{provider}.json'
        with open(path_to_json, 'r') as j:
            contents = json.loads(j.read())
            data = contents.get('data')
            for item in range(0, 100):
                random_item = data[item]
                if random_item["code"] != None:
                    if random_item["description"] != '':
                        if random_item["price"] != 'NaN':
                            item_to_create = {
                                "itemCode": random_item["code"],
                                "itemDescription": random_item["description"],
                                "itemPrice": Decimal(random_item["price"]),
                                "itemProvider": provider
                            }
                            if provider == 'tolken':
                                logger.info(item_to_create)
                # logger.info(item_to_create)
                array_of_items.append(item_to_create)
                print(f'generating item number: {item}')
        j.close()
    for item in range(0, len(array_of_items) -1):
        # logger.info(f'loading item in database: {item}')
        logger.info(array_of_items[item])
        table.put_item(Item=array_of_items[item])
    logger.info('Inserted 100 items per provider into DynamoDB')


def delete_dynamo_data(logger):
    logger.info('Deleting all DynamoDB data')
    if _already_seeded(logger):
        logger.info('Table seeded, removing data....')
        try:
            table = ddb.Table(PRODUCTS_TABLE_NAME)
            table.delete()
            logger.info(f"Deleting {table.name}...")
            table.wait_until_not_exists()
        except ClientError as error:
            if error.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info('Table may not exists or it is already removed')
                pass
            else:
                logger.info('Table Removed')

    