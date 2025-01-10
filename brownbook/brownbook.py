import aiohttp
import asyncio
import pandas as pd
from urllib.parse import quote
import logging

# Simple logging setup at the top of file
logging.basicConfig(
    filename='brownbook_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)

async def get_business_details(session, business_id):
    url = f"https://api.brownbook.net/app/api/v1/business/{business_id}/fetch"
    
    headers = {
        "accept": "application/json",
        "origin": "https://www.brownbook.net",
        "referer": "https://www.brownbook.net/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        result = data.get('data', {})
        if not result:
            logging.warning(f"Empty result for: {url}")
        return result
    

def parse_business_details(data):
    metadata = data.get('metadata', {})
    tags_list = metadata.get('tags', [])
    tags = [tag.get('name') for tag in tags_list if tag.get('name')]
    tags_string = ', '.join(tags)

    service_list = metadata.get('services', [])
    services = [service.get('name') for service in service_list if service.get('name')]
    service_string = ', '.join(services)
    
    return {
        'id': metadata.get('id'),
        'name': metadata.get('name'),
        'phone': metadata.get('phone'),
        'mobile': metadata.get('mobile'),
        'address_line_1': metadata.get('address_line_1'),
        'city': metadata.get('city'),
        'state': metadata.get('state'),
        'email': metadata.get('email'),
        'zip_code': metadata.get('zip_code'),
        'website': metadata.get('website'),
        'facebook': metadata.get('facebook'),
        'tags': tags_string,   # business type
        'services': service_string
    }

async def scrape_page_businesses(session, businesses):
    tasks = []
    for business in businesses:
        business_id = business['id']
        tasks.append(get_business_details(session, business_id))
    
    details = await asyncio.gather(*tasks)
    return [parse_business_details(detail) for detail in details]


async def scrape_brownbook(business_type, country_code="us"):
    url = f"https://api.brownbook.net/app/api/v1/businesses/search/by-name/{quote(business_type)}"
    
    headers = {
        "accept": "application/json",
        "origin": "https://www.brownbook.net",
        "referer": "https://www.brownbook.net/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    }
    
    all_businesses = []
    
    async with aiohttp.ClientSession() as session:
        for page in range(1, 334):  # max results is 10000
            params = {
                "page": str(page),
                "country_code": country_code,
                "city": "all-cities"
            }
            
            async with session.get(url, headers=headers, params=params) as response:
                data = await response.json()
                businesses = data['data']['businesses']
                
                page_businesses = await scrape_page_businesses(session, businesses)
                all_businesses.extend(page_businesses)
                
                print(f"Scraped page {page}: {len(businesses)} businesses found")

    # Create DataFrame first
    df = pd.DataFrame(all_businesses)
    
    # Drop rows where all columns are empty/null
    df_cleaned = df.dropna(subset=['id'])
    
    filename = f"{business_type.replace(' ', '_')}_{country_code}.csv"
    df_cleaned.to_csv(filename, index=False, encoding='utf-8')
    
    print(f"Total businesses found: {len(all_businesses)}")
    print(f"Total valid businesses: {len(df_cleaned)}")
    print(f"Data saved to {filename}")
    return 


async def main():
    business_types = ['yoga studio']  # , 'fitness', 'swimming'
    countries = ['us']  # , 'uk'
    # all_data = {}
    
    for business in business_types:
        for country in countries:
            print(f"\nScraping {business} businesses in {country}")
            await scrape_brownbook(business, country)


if __name__ == "__main__":
    asyncio.run(main())


# notes
# - https://www.brownbook.net/business/xxx where xxx is the business id lands on the business page