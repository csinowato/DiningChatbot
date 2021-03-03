import json
import boto3
import datetime

def lambda_handler(event, context):
    messages = event['messages'][0]
    text = messages['unstructured']['text']

    # Call Lex Chatbot
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='DiningChatbot',
        botAlias='prod',
        userId='user',
        inputText=text #send user input to lex chatbot
    )

    # get the response from lex
    lexResponse = response['message']

    # send lex response to the user
    response = {
        "messages": [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": 'user', #there's only 1 user, so hardcode a userID
                    "text": lexResponse
                }
            }
        ]
    }


    return {
        'headers': {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        # 'body': json.dumps("I'm still under development. Please come back later"),
        'messages': response['messages']
    }
