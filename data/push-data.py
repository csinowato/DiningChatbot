import json
import boto3
import datetime

TABLE_NAME = 'yelp-restaurants'

restaurants = json.load(open("data/restaurantdata.txt"))

# Send the yelp restaurant data to DynamoDB
dynamodb = boto3.resource('dynamodb')
dynamoTable = dynamodb.Table(TABLE_NAME)

for resto in restaurants:
    print(resto['name'])

    dynamoTable.put_item(
        Item = {
            'businessID': resto['businessID'],
            'name': resto['name'],
            'cuisine_type': resto['cuisine_type'],
            'address': resto['address'],
            'coordinates': resto['coordinates'],
            'review_count': resto['review_count'],
            'rating': str(resto['rating']),
            'zip_code': resto['zip_code'],
            'insertedAtTimestamp': datetime.datetime.now().isoformat(),
        }
    )
