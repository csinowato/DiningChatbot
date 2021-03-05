import json
import boto3
import random
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key


# DynamoDB Table
TABLE_NAME = 'yelp-restaurants'

# ElasticSearch Instance
host = 'search-restaurant-data-lsfrwxfaj7jekygcgpm46thgru.us-east-1.es.amazonaws.com'
region = 'us-east-1'

# SQS Queue
sqs = boto3.client('sqs')
queueURL = 'https://sqs.us-east-1.amazonaws.com/280139081269/Q1'


def lambda_handler(event, context):
    suggestions = ''

    # Fetch messages from SQS
    response = sqs.receive_message(
        QueueUrl = queueURL,
        MessageAttributeNames=['All'],
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
        )
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html

    if 'Messages' in response:
        message_attributes = response['Messages'][0]['MessageAttributes']

        # Get attributes from message
        location = message_attributes['location']['StringValue']
        cuisine = message_attributes['cuisine']['StringValue']
        number_of_people = message_attributes['number_of_people']['StringValue']
        dining_date = message_attributes['dining_date']['StringValue']
        dining_time = message_attributes['dining_time']['StringValue']
        phone_number = message_attributes['phone_number']['StringValue']
        print('Cuisine-->', cuisine)


        # Elastic search auth
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

        # Connect to elastic search
        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )
        # https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-request-signing.html#es-request-signing-python


        # Use elasticsearch to store restaurants that meet user's criteria
        valid_restaurants = []

        # Query elastic seach
        # Get size first (total number of hits for that cuisine)
        result_size = es.search(index="restaurants", body={"query": {"match": {'cuisine': cuisine}}})
        num_hits = result_size['hits']['total']['value']

        res = es.search(index="restaurants", body={"query": {"match": {'cuisine': cuisine}}}, size=num_hits)
        for hit in res['hits']['hits']:
            valid_restaurants.append(hit['_id'])
        # https://elasticsearch-py.readthedocs.io/en/v7.11.0/
        # https://elasticsearch-py.readthedocs.io/en/v7.11.0/async.html?highlight=size#getting-started-with-async


        # Make sure there are at least 3 valid restaurants:
        if len(valid_restaurants) < 3:
            return {
                'statusCode': 200,
                'body': json.dumps("Not enough restaurants for this cuisine type were available.")
            }

        # Randomly select 3 restaurants
        random_idx = random.sample(range(len(valid_restaurants)-1),3) # use this when random_restaurants is populated
        random3 = []
        for i in random_idx:
            random3.append(valid_restaurants[i])


        # Use the businessIDs from Elastic Search to query DynamoDB to get detailed info for each restaurant
        restaurant_details = []
        for resto in random3:
            restaurant_details.append(query_database(resto)[0])


        # Generate text to send to user
        plural = ' person' if number_of_people == '1' else ' people'
        suggestions = "Hello! Here are my " + cuisine.title() + " restaurant suggestions for " + number_of_people + plural + " for " + dining_date + " at " + dining_time + ": "
        for i in range(3):
            suggestions += str(i+1) + ". " + restaurant_details[i]['name'] + " located at " + restaurant_details[i]['address']['display_address'][0]
            suggestions += ', ' if i <2 else '. '
        suggestions += "Enjoy your meal!"

        print("SUGGESTIONS:", suggestions)


        # Send suggestions to user
        text_user(phone_number, suggestions)


        # Delete message from SQS queue
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        delete_message_from_queue(queueURL, receipt_handle)


    return {
        'statusCode': 200,
        'body': json.dumps(suggestions)
    }


def query_database(businessID):
        dynamodb = boto3.resource('dynamodb')
        dynamoTable = dynamodb.Table(TABLE_NAME)
        response = dynamoTable.query(
            KeyConditionExpression=Key('businessID').eq(businessID)
        )
        return response['Items']
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html


def text_user(phone_number, message):
    try:
        sns = boto3.client('sns')
        sns.publish(PhoneNumber='+1' + phone_number, Message = message)
    except:
        return "Invalid phone number."
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns.html#SNS.Client.publish
# https://docs.aws.amazon.com/sns/latest/dg/sms_publish-to-phone.html


def delete_message_from_queue(queueURL, receipt_handle):
    sqs.delete_message(
        QueueUrl=queueURL,
        ReceiptHandle=receipt_handle
    )
# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
