import json
import datetime
import time
import os
import dateutil.parser
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


# --- Helpers that build all of the responses ---


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response


def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }


# --- Helper Functions ---


def safe_int(n):
    """
    Safely convert n value to int.
    """
    if n is not None:
        return int(n)
    return n


def try_ex(func):
    """
    Call passed in function in try block. If KeyError is encountered return None.
    This function is intended to be used to safely access dictionary.

    Note that this function would have negative impact on performance.
    """
    try:
        return func()
    except KeyError:
        return None


# --- Functions to Validate Each Slot ---


def isvalid_location(location):
    valid_locations = ['new york', 'newyork', 'ny', 'new york city', 'nyc']
    return location.lower() in valid_locations

def isvalid_cuisine(cuisine):
    valid_cusines = ['japanese', 'italian', 'chinese', 'american', 'indian', 'korean', 'vietnamese', 'thai', 'vegan']
    return cuisine.lower() in valid_cusines

def isvalid_date(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

def isvalid_time(time):
    try:
        datetime.datetime.strptime(time, '%H:%M')
        return True
    except ValueError:
        return False

def isvalid_phone_number(phone_number):
    return len(str(phone_number)) == 10


# --- Functions to Validate Booking ---


def build_validation_result(isvalid, violated_slot, message_content):
    return {
        'isValid': isvalid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message_content}
    }


def validate_book_restaurant(slots):
    location = try_ex(lambda: slots['location'])
    cuisine = try_ex(lambda: slots['cuisine'])
    number_of_people = safe_int(try_ex(lambda: slots['numberOfPeople']))
    dining_date = try_ex(lambda: slots['diningDate'])
    dining_time = try_ex(lambda: slots['diningTime'])
    phone_number = try_ex(lambda: slots['phoneNumber'])

    if location and not isvalid_location(location):
        return build_validation_result(
            False,
            'location',
            'Sorry, we do not support {}. We currently only support New York'.format(location)
        )

    if dining_date:
        if not isvalid_date(dining_date):
            return build_validation_result(False, 'diningDate', 'I did not understand your dining date. When would you like to dine?')
        if datetime.datetime.strptime(dining_date, '%Y-%m-%d').date() < datetime.date.today():
            return build_validation_result(False, 'diningDate', 'Reservations cannot be scheduled for past dates. Can you try a different date?')

    if dining_time:
        if not isvalid_time(dining_time):
            return build_validation_result(False, 'diningTime', 'I did not understand your dining time. What time would you like to dine?')
        if datetime.datetime.strptime(dining_date+'-'+dining_time, '%Y-%m-%d-%H:%M') <= datetime.datetime.now():
            return build_validation_result(False, 'diningTime', 'Reservations must be scheduled for future times. Can you try a different time?')

    if phone_number and not isvalid_phone_number(phone_number):
        return build_validation_result(False, 'phoneNumber','Please enter a valid 10 digit phone number.')

    if number_of_people:
        if number_of_people <= 0:
            return build_validation_result(False, 'numberOfPeople', 'The number of people in your party must be positive. How many people are in your party?')
        if number_of_people > 20:
            return build_validation_result(False, 'numberOfPeople', 'The number of people in your party must be less than 20. How many people are in your party?')

    if cuisine and not isvalid_cuisine(cuisine):
        return build_validation_result(
            False,
            'cuisine',
            'I did not recognize that cuisine. What cuisine would you like to try? Please choose from the following: Japanese, Italian, Chinese, American, Indian, Korean, Vietnamese, Thai, Vegan.'
        )

    return {'isValid': True}



""" --- Functions that control the bot's behavior --- """


def book_restaurant(intent_request):
    """
    Performs dialog management and fulfillment for booking a car.

    Beyond fulfillment, the implementation for this intent demonstrates the following:
    1) Use of elicitSlot in slot validation and re-prompting
    2) Use of sessionAttributes to pass information that can be used to guide conversation
    """
    slots = intent_request['currentIntent']['slots']

    location = try_ex(lambda: slots['location'])
    cuisine = try_ex(lambda: slots['cuisine'])
    number_of_people = safe_int(try_ex(lambda: slots['numberOfPeople']))
    dining_date = try_ex(lambda: slots['diningDate'])
    dining_time = try_ex(lambda: slots['diningTime'])
    phone_number = try_ex(lambda: slots['phoneNumber'])


    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}

    if intent_request['invocationSource'] == 'DialogCodeHook':
        # Validate any slots which have been specified.  If any are invalid, re-elicit for their value
        validation_result = validate_book_restaurant(intent_request['currentIntent']['slots'])
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                validation_result['violatedSlot'],
                validation_result['message']
            )

        return delegate(session_attributes, intent_request['currentIntent']['slots'])


    # Push the information collected to an SQS queue:
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName = 'Q1')

    queue.send_message(MessageBody='user_info', MessageAttributes={
        'location': {
            'StringValue': location,
            'DataType': 'String'
        },
        'cuisine': {
            'StringValue': cuisine,
            'DataType': 'String'
        },
        'number_of_people': {
            'StringValue': str(number_of_people),
            'DataType': 'String'
        },
        'dining_date': {
            'StringValue': dining_date,
            'DataType': 'String'
        },
        'dining_time': {
            'StringValue': dining_time,
            'DataType': 'String'
        },
        'phone_number': {
            'StringValue': str(phone_number),
            'DataType': 'String'
        }
    })


    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': "You're all set. Expect my suggestions shortly! Have a good day."
        }
    )


# --- Intents ---


def dispatch(intent_request):
    """
    Called when the user specifies an intent for this bot.
    """

    logger.debug('dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))

    intent_name = intent_request['currentIntent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return book_restaurant(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    """
    Route the incoming request based on intent.
    The JSON body of the request is provided in the event slot.
    """
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))

    return dispatch(event)
