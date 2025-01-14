import requests
from dotenv import load_dotenv
import os
import pandas as pd


# Load API key from .env file
load_dotenv()
API_KEY = os.getenv('YELP_FUSION_KEY')

# Yelp API base URL or the search endpoint
BASE_URL = "https://api.yelp.com/v3/businesses/search"

# HTTP headers for authentication
HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'accept': 'application/json'
}

def get_business_list(term, categories, location):
    """
    Query the Yelp API to fetch businesses in a specific city.

    Args:
        term (str): The term to search for.
        categories (str): The categories to filter by and get the most specific results.
        location (str): The location to search in.
    """
    total_business_list = []
    for offset in range(0, 1000, 50):
        params = {
            'term': term,
            'location': location,
            'categories': categories,
            'limit': 50,
            'offset': offset,
        }
        response = requests.get(BASE_URL, headers=HEADERS, params=params)

        # if the response is not 200, we assume that the offset has exceeded the total number of businesses
        if response.status_code != 200:  
            break
        business_list = response.json().get('businesses')
        total_business_list.extend(business_list)

        if len(business_list) < 50:  # indicates the last page
            break

    return total_business_list

         

def parse_business_data(business):
    """
    Parse relevant fields from the business JSON object.
    """
    id = business.get('id', 'N/A')
    name = business.get('name', 'N/A')
    phone = business.get('phone', 'N/A')
    
    # get address from the location key
    location = business.get('location')
    address1 = location.get('address1', 'N/A')  
    zip_code = location.get('zip_code', 'N/A')
    city = location.get('city', 'N/A')
    state = location.get('state', 'N/A')
    country = location.get('country', 'N/A')

    # get title of categories from the category key
    categories = business.get('categories')
    categories = [category.get('title', 'N/A') for category in categories]
    category = '|'.join(categories)

    # get some info from the attributes key
    attributes = business.get('attributes')
    business_url = attributes.get('business_url', 'N/A')
    hot_and_new = attributes.get('hot_and_new', 'N/A')

    return {
        'id': id,
        'Name': name,
        'Website': business_url,
        'Phone': phone,
        'Street': address1,
        'Zip Code': zip_code,
        'City': city,
        'State': state,
        'Country': country,
        'Category': category,
        'Hot and New': hot_and_new
    }


def main():
    term = 'Gyms'  # i probably have to never change this term on yelp
    location = 'Hampden County, MA'

    categories_list = pd.read_csv('categories.csv', header=None)[0].tolist()
    all_categories_data = []
    
    for categories in categories_list:  # the categories is expected by the Yelp API, not category
        parsed_businesses_list = []
        business_list = get_business_list(term, categories, location)
        
        for business in business_list:
            parsed_business = parse_business_data(business)
            parsed_businesses_list.append(parsed_business)
            print(parsed_business)

        all_categories_data.extend(parsed_businesses_list)

    df = pd.DataFrame(all_categories_data)
    df.to_csv('Hampden County.csv', index=False)

        # df = pd.DataFrame(parsed_businesses_list)
        # df.to_csv(f'{categories}.csv', index=False)




if __name__ == '__main__':
    main()
