import os
import json
import boto3
import constants
import utils
from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, Any, List
from dataclasses import dataclass
from collections import defaultdict

# Create the logger for this module
logger = utils.create_logger(__name__)

# Constants used in DynamoDB requests and responses  
ITEM_KEY = 'Item'
ITEMS_KEY = 'Items'
LAST_EVALUATED_KEY = 'LastEvaluatedKey'
RESPONSE_METADATA_KEY = 'ResponseMetadata'
HTTP_STATUS_CODE_KEY = 'HTTPStatusCode'
HTTP_STATUS_OK = 200
TABLE_NAME_KEY = 'TableName'
PUT_KEY = 'Put'

# Table Columns
TABLE_PARTITION_KEY = 'table_partition_key'
TABLE_SORT_KEY = 'table_sort_key'
PAYLOAD_KEY = 'payload'
DELETE_TIMESTAMP_KEY = 'delete_timestamp_utc'

# Prefixes & Constants for table column values
TABLE_SORT_KEY_PREFIX_LATEST = 'latest_'
TABLE_PARTITION_KEY_SAMPLE_DATA = 'sample_data_'

@dataclass
class QueryResult:
    partition_key: str
    sort_key: str
    payload: Dict[str, Any]


class Repository(object):
    def __init__(self, is_offline: bool, service_table_name: str):
        self.resource = self.create_resource(is_offline)
        self.client = self.create_client(is_offline)
        self.service_table = self.resource.Table(service_table_name)
        self.type_serializer = boto3.dynamodb.types.TypeSerializer()

    #
    # Generic functions for reading, saving and querying items   
    #
    def query_items(self, partition_key: str, sort_key_prefix: str = None) -> List[QueryResult]:
        logger.info(f'querying items by sort key prefix partition_key = {partition_key}, sort_key_prefix = {sort_key_prefix}')
        
        projection_expression = f'{TABLE_SORT_KEY},{PAYLOAD_KEY}'
        key_condition_expr = Key(TABLE_PARTITION_KEY).eq(partition_key)
        if sort_key_prefix:
            key_condition_expr = key_condition_expr & Key(TABLE_SORT_KEY).begins_with(sort_key_prefix)

        results = []

        last_evaluated_key = None
        while True:
            if last_evaluated_key is not None:
                response = self.service_table.query(KeyConditionExpression=key_condition_expr, ProjectionExpression=projection_expression, ExclusiveStartKey=last_evaluated_key)
            else:
                response = self.service_table.query(KeyConditionExpression=key_condition_expr, ProjectionExpression=projection_expression)   
        
            for item in response[ITEMS_KEY]: 
                results.append(QueryResult(partition_key=partition_key, sort_key=item[TABLE_SORT_KEY], payload=item[PAYLOAD_KEY]))

            # Check if there are more items to be queries
            last_evaluated_key = response.get(LAST_EVALUATED_KEY)
            # Exit if the scan has completed
            if last_evaluated_key is None:
                break

        return results


    def read_audited_item_lastest(self, partition_key: str, sort_key_suffix: str) -> Dict[str, Any]:
        logger.info(f'reading lastest audited item partition_key = {partition_key}, sort_key_suffix = {sort_key_suffix}')
        
        response = self.service_table.get_item(Key={TABLE_PARTITION_KEY: partition_key, 
                                                   TABLE_SORT_KEY: f'{TABLE_SORT_KEY_PREFIX_LATEST}{sort_key_suffix}'},
                                               ProjectionExpression=f'{PAYLOAD_KEY}')
        if ITEM_KEY in response:
            return response[ITEM_KEY][PAYLOAD_KEY]
        else:
            logger.warn(f'unsuccessful reading lastest audited item partition_key = {partition_key}, sort_key_suffix = {sort_key_suffix}')
            return None


    def save_audited_item(self, partition_key: str, sort_key_suffix: str, timestamp: str, payload: Dict[str, Any], delete_ts: int = None) -> bool:
        logger.info(f'saving audited item partition_key = {partition_key}, sort_key_suffix = {sort_key_suffix}, timestamp = {timestamp}')

        # Going to put 2 rows in the table both having the same partition key but different sort keys
        payload_serialized = self.type_serializer.serialize(payload)

        latest_put_item = {PUT_KEY: {ITEM_KEY: {TABLE_PARTITION_KEY: {'S': partition_key},
                                                TABLE_SORT_KEY: {'S': f'{TABLE_SORT_KEY_PREFIX_LATEST}{sort_key_suffix}'},
                                                PAYLOAD_KEY: payload_serialized},
                                     TABLE_NAME_KEY: self.service_table.name}}


        datetime_put_item = {PUT_KEY: {ITEM_KEY: {TABLE_PARTITION_KEY: {'S': partition_key},
                                                  TABLE_SORT_KEY: {'S': f'{timestamp}_{sort_key_suffix}'},
                                                  PAYLOAD_KEY: payload_serialized},
                                       TABLE_NAME_KEY: self.service_table.name}}

        if delete_ts and isinstance(delete_ts, int):
            datetime_put_item[PUT_KEY][ITEM_KEY][DELETE_TIMESTAMP_KEY] = {'N': str(delete_ts)} # numbers are passed as strings in dynamodb

        response = self.client.transact_write_items(TransactItems=[latest_put_item, datetime_put_item])
        if response[RESPONSE_METADATA_KEY][HTTP_STATUS_CODE_KEY] == HTTP_STATUS_OK:
            return True
        else:
            logger.warn(f'unsuccessful saving audited item partition_key = {partition_key}, sort_key_suffix = {sort_key_suffix}, timestamp = {timestamp}')
            return False


    def read_item(self, partition_key: str, sort_key: str) -> Dict[str, Any]:
        logger.info(f'reading item partition_key = {partition_key}, sort_key = {sort_key}')
        
        response = self.service_table.get_item(Key={TABLE_PARTITION_KEY: partition_key, 
                                                    TABLE_SORT_KEY: sort_key},
                                               ProjectionExpression=f'{PAYLOAD_KEY}')
        if ITEM_KEY in response:
            return response[ITEM_KEY][PAYLOAD_KEY]
        else:
            logger.warn(f'unsuccessful reading item partition_key = {partition_key}, sort_key = {sort_key}')
            return None


    def save_item(self, partition_key: str, sort_key: str, payload: Any, delete_ts: int = None) -> bool:
        logger.info(f'saving item partition_key = {partition_key}, sort_key = {sort_key}')

        item = {TABLE_PARTITION_KEY: partition_key,
                TABLE_SORT_KEY: sort_key,
                PAYLOAD_KEY: payload}
        if delete_ts:
            item[DELETE_TIMESTAMP_KEY] = delete_ts

        response = self.service_table.put_item(Item=item)
        if response[RESPONSE_METADATA_KEY][HTTP_STATUS_CODE_KEY] == HTTP_STATUS_OK:
            return True
        else:
            logger.warn(f'saving item partition_key = {partition_key}, sort_key = {sort_key}')
            return False

    #
    # Create connections to access DynamoDB
    #

    @classmethod
    def create_resource(cls, is_offline):
        if is_offline:
            logger.info('connecting to offline dynamodb resource')
            return boto3.resource('dynamodb', region_name='localhost', endpoint_url='http://localhost:8000')
        else:
            logger.info('connecting to online dynamodb resource')
            return boto3.resource('dynamodb')

    @classmethod
    def create_client(cls, is_offline):
        if is_offline:
            logger.info('connecting to offline dynamodb client')
            return boto3.client('dynamodb', region_name='localhost', endpoint_url='http://localhost:8000')
        else:
            logger.info('connecting to online dynamodb client')
            return boto3.client('dynamodb')

    #
    # CRUD for Sample Data
    #
    