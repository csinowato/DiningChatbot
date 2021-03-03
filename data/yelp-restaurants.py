import requests
import json
from config import YELP_API_KEY

API_KEY = YELP_API_KEY
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

cuisines = ['japanese', 'italian', 'chinese', 'american', 'indian', 'korean', 'vegan']
ranges = {'japanese': 20, 'italian': 20, 'chinese': 20, 'american':20, 'indian': 10, 'korean':20, 'vegan':20}
restaurants_all = []

offset_val = 0
for cuisine in cuisines:
    # can only get 50 restaurants at a time so loop many times updating the offset each time

    for offset_val in range(ranges[cuisine]):
        PARAMETERS = {'term': cuisine,
                    'limit': 50, # max limit
                    'offset': offset_val * 50,
                    'radius': 10000,
                    'location': 'NY'}

        # Make a request to yelp API
        response = requests.get(url= ENDPOINT, params= PARAMETERS, headers= HEADERS)
        restaurant_data = response.json()
        print(cuisine, offset_val)

        # convert JSON string to dictionary
        for restaurant in restaurant_data['businesses']:
            restaurants_all.append({
                            'businessID': restaurant['id'],
                            'name': restaurant['name'],
                            'cuisine_type': cuisine, # add cuisine type
                            'address': restaurant['location'],
                            # convert coordinates to str because boto3 doesn't support float
                            'coordinates': {'latitude': str(restaurant ['coordinates']['latitude']),
                                            'longitude': str(restaurant ['coordinates']['longitude'])},
                            'review_count': restaurant['review_count'],
                            'rating': restaurant['rating'],
                            'zip_code': restaurant['location']['zip_code']
                            })


restaurants_set = set()
unique_restaurants = []

# Keep only unique restaurants (remove duplicates)
r = 0
for r in restaurants_all:
    if r['businessID'] not in restaurants_set:
        restaurants_set.add(r['businessID'])
        unique_restaurants.append(r)

# make sure there are over 5000 unique restaurants
print("Number of unique restaurants:", len(unique_restaurants))

# save restaurant data to a file
json.dump(unique_restaurants, open("data/restaurantdata.txt", "w"))


# save restaurantID and cuisine in json file for elastic search
restaurants_json = []
for r in unique_restaurants:
    restaurants_json.append({"index":{"_index":"restaurants","_type":"Restaurant","_id":r['businessID']}})
    restaurants_json.append({"cuisine": r['cuisine_type']})

with open('data/restaurantelastic.json', 'w') as outfile:
    for i in restaurants_json:
        out = json.dumps(i, separators=(',',':'))
        outfile.write(out + '\n')
