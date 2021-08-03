#!/usr/bin/env python
import sys
import os
from time import sleep

import boto3

if len(sys.argv) != 3:
    print("Usage: %s <source_table_name> <destination_table_name>" % sys.argv[0])
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]
region = os.getenv('AWS_DEFAULT_REGION', 'us-west-2')
source_profile = os.getenv('AWS_SOURCE_PROFILE', 'default')
destination_profile = os.getenv('AWS_DESTINATION_PROFILE', source_profile)

print("Source profile ", source_profile)
print("Destination profile ", destination_profile)

source_session = boto3.session.Session(profile_name=source_profile, region_name=region)
ddbsc = source_session.client("dynamodb")
if source_profile == destination_profile:
    ddbdc = ddbsc
else:
    destination_session = boto3.session.Session(profile_name=destination_profile, region_name=region)
    ddbdc = destination_session.client("dynamodb")


# 1. Read and copy the target table to be copied
table_struct = None
try:
    table_struct = ddbsc.describe_table(TableName=src_table)
    print("table struct ", table_struct)
except ddbsc.exceptions.ResourceNotFoundException:
    print("Table %s does not exist" % src_table) 
    sys.exit(1)

print("*** Reading key schema from %s table" % src_table)
src = table_struct['Table']
hash_key = ''
range_key = ''
for schema in src['KeySchema']:
    attr_name = schema['AttributeName']
    key_type = schema['KeyType']
    if key_type == 'HASH':
        hash_key = attr_name
    elif key_type == 'RANGE':
        range_key = attr_name
print("source schema ", src['KeySchema'])

print("hash key ", hash_key)
print("range key ", range_key)

# 2. Create the new table
table_struct = None
try:
    table_struct = ddbdc.describe_table(TableName=dst_table)
    print("dest table struct ", table_struct)
    if 'DISABLE_CREATION' in os.environ:
        print("Creation of new table is disabled. Skipping...")
    else:
        print("Table %s already exists" % dst_table)
        sys.exit(0)
except ddbdc.exceptions.ResourceNotFoundException:
    schema = [
                 {
                        'AttributeName': hash_key,
                        'KeyType': 'HASH'
                    },

                ]
    attributeDefinitions = [
        {
            'AttributeName': hash_key,
            'AttributeType': 'S'
        },
    ]

    if range_key != '':
        schema.append({
                        'AttributeName': range_key,
                        'KeyType': 'RANGE'
                    })
        attributeDefinitions.append({
            'AttributeName': range_key,
            'AttributeType': 'S'
        })
    billing_info = {'BillingMode': src['BillingModeSummary']['BillingMode']}
    readCapacity = src['ProvisionedThroughput']['ReadCapacityUnits']
    writeCapacity = src['ProvisionedThroughput']['WriteCapacityUnits']
    if readCapacity > 0 and writeCapacity > 0:
        billing_info['ProvisionedThroughput'] = {
                                      'ReadCapacityUnits': src['ProvisionedThroughput']['ReadCapacityUnits'],
                                      'WriteCapacityUnits': src['ProvisionedThroughput']['WriteCapacityUnits'],
                                  }

    new_logs = ddbdc.create_table(TableName=dst_table,
                                  KeySchema=schema,
                                  AttributeDefinitions=attributeDefinitions,
                                  **billing_info
                                  )
    print("*** Waiting for the new table %s to become active" % dst_table)
    sleep(5)
    while ddbdc.describe_table(TableName=dst_table)['Table']['TableStatus'] != 'ACTIVE':
        sleep(3)

if 'DISABLE_DATACOPY' in os.environ:
    print("Copying of data from source table is disabled. Exiting...")
    sys.exit(0)

# 3. Add the items
for item in ddbsc.scan(TableName=src_table)['Items']:
    new_item = {}
    new_item[hash_key] = item[hash_key]
    if range_key != '':
        new_item[range_key] = item[range_key]
    for f in item.keys():
        if f in [hash_key, range_key]:
            continue
        new_item[f] = item[f]
    try:
        print("new item ", new_item)
        ddbdc.put_item(TableName=dst_table, Item=new_item)
    except ddbdc.exceptions.ValidationException:
        print(dst_table, new_item)

print("We are done. Exiting...")
