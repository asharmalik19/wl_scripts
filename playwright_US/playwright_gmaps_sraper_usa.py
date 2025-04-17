from playwright.sync_api import sync_playwright
import re
import pandas as pd
from datetime import datetime
import logging
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError

def search(page, search_query):
    """Returns True if search results are found, False if redirected to a single business.
    Also checks for Google Captcha and raises an exception if detected.
    """
    page.goto('https://www.google.com/maps')

    # Check for captcha before proceeding
    if page.query_selector('div#captcha-form'):
        logging.error("Google Captcha detected - automation blocked")
        raise Exception("Google Captcha detected - please resolve captcha or try again later")
    
    page.fill('#searchboxinput', search_query)
    page.keyboard.press('Enter')
    page.wait_for_load_state('networkidle')

    # Check for captcha after search
    if page.query_selector('div#captcha-form'):
        logging.error("Google Captcha detected after search - automation blocked")
        raise Exception("Google Captcha detected - please resolve captcha or try again later")

    if page.query_selector('h1.DUwDvf.lfPIob'):
        logging.info(f"Search '{search_query}' redirected to single business - skipping keyword")
        print(f"Search '{search_query}' redirected to single business - skipping keyword")
        return False
    page.wait_for_selector(f"div[aria-label='Results for {search_query}']", timeout=120000)
    return True

def get_links(page):
    results = page.query_selector_all('a.hfpxzc')
    result_links = [result.get_attribute('href') for result in results]
    return result_links

def scroll(page, search_query):
    sidebar = page.query_selector(f"div[aria-label='Results for {search_query}']")
    while True:
        sidebar.press('PageDown')
        page.wait_for_timeout(500)
        sidebar.press('PageDown') 
        page.wait_for_timeout(500)   
        if 'reached the end of the list' in page.content():
            break
    return

# TODO: Add a single retry here
def get_business_page_source(page, link):
    try:
        page.goto(link, timeout=30000)
        page.wait_for_load_state('networkidle', timeout=30000)
        return page.content()
    except TimeoutError:
        logging.error(f"Timeout while loading {link}")
        return None
    
def scrape_business_details(page_source):  
    soup = BeautifulSoup(page_source, 'html.parser')
    
    business_title = soup.select_one('h1.DUwDvf.lfPIob').text.strip()
    business_title = business_title.replace('"', '').replace("'", '')

    business_type_elem = soup.select_one('button.DkEaL')
    business_type = business_type_elem.text.strip() if business_type_elem else ''

    address_elem = soup.select_one('button[data-item-id="address"] div.rogA2c')
    address = address_elem.text.strip() if address_elem else ''

    website_link_elem = soup.select_one('a[data-tooltip="Open website"]')
    website_link = website_link_elem.get('href') if website_link_elem else ''

    domain_pattern = re.compile(r'\b(?:https?://)?(?:www\.)?([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b')
    number_pattern = re.compile(r'(?:\+\d{1,2}\s)?\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})')

    closed_business_elem = soup.select_one('span.fCEvvc')
    business_status = closed_business_elem.text.strip() if closed_business_elem else ''

    contact_details_div = soup.select_one(f'div[aria-label="Information for {business_title}"]')
    contact_details_text = contact_details_div.text if contact_details_div else ''

    domain_name = domain_pattern.search(contact_details_text)
    domain_name = domain_name.group() if domain_name else ''

    number = number_pattern.search(contact_details_text)
    number = number.group() if number else ''

    business_timings = get_business_timings(page_source)
    business_details = {
        'Company': business_title,
        'Number': number,
        'Address': address,
        'Domain': domain_name,
        'Website': website_link,
        'Business Type': business_type,
        'Business Status': business_status,

    }
    business_details.update(business_timings)
    return business_details

def make_search_query(keyword, location):
    keyword = keyword + ' in '
    location = ', '.join(i for i in location.values())
    search_query = keyword + location
    return search_query

def remove_duplicates(data_df):
    columns_to_check = ['Company', 'Number', 'Address']
    data_df_without_duplicates = data_df[~data_df.duplicated(subset=columns_to_check, keep='first')]
    return data_df_without_duplicates

def get_business_timings(page_source):
    VALID_KEYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    soup = BeautifulSoup(page_source, "html.parser")
    business_hours = {}
    table = soup.find("table", class_="eK4R0e")  
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")[:2]
            # sometimes there are name variations stored as multiple divs in a single cell td
            day = cells[0].find("div").get_text(strip=True)
            timings = cells[1].find("li").get_text(strip=True)
            timings = re.sub(r'\s+', ' ', timings, flags=re.UNICODE)
            business_hours.update({day: timings})
 
    for key in business_hours.keys():
        if key not in VALID_KEYS:
            logging.error(f"Data Quality Error: Invalid day found - {key}")
            raise ValueError(f"Data Quality Error: Invalid day found - {key}")
    return business_hours

if __name__ == '__main__':
    start_time = datetime.now()
    df = pd.read_csv('keywords.csv')
    keyword_list = df.loc[df['status'].str.lower() == 'on', 'keywords'].to_list()
    cities_df = pd.read_csv('cities.csv')
    cities = cities_df['City'].to_list()

    logging.basicConfig(filename='g_map_scraper.log', filemode='w', level=logging.INFO)
    
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for city in cities:
            LOCATION = {
                'City': city,
                'State': 'TN',
                'Country': 'USA'
            }
            data_df = pd.DataFrame(columns=[
                'Company', 'Number', 'Address', 'Domain', 'Website',
                'Business Type', 'Business Status',
                'Monday', 'Tuesday', 'Wednesday',
                'Thursday', 'Friday', 'Saturday', 'Sunday', 
                'G-map link', 'State', 'Keyword'
            ])

            for keyword in keyword_list:
                search_query = make_search_query(keyword=keyword, location=LOCATION)
                # handles the case when the search is redirected to a business page
                if not search(page=page, search_query=search_query):
                    continue
                scroll(page=page, search_query=search_query)
                result_links = get_links(page=page)

                for link in result_links:
                    page_source = get_business_page_source(page=page, link=link)
                    # if a business page takes too long to load or fails to load, skip it
                    if not page_source:
                        continue
                    business_details = scrape_business_details(page_source=page_source)
                    business_details.update({
                        'G-map link': link,
                        'State': LOCATION['State'],
                        'Keyword': keyword
                    })
                    data_df.loc[len(data_df)] = business_details
                    print(f'Scraped: {business_details}')
                    print('-' * 50)

            data_df_without_duplicates = remove_duplicates(data_df=data_df)
            data_df_without_duplicates.to_excel(f'{city}.xlsx', index=False)
            logging.warning(f'Scraping completed for {city}. Total execution time: {datetime.now() - start_time}')
            print(f'Completed scraping {city}')

        browser.close()
    print(f'Total execution time: {datetime.now() - start_time}')

