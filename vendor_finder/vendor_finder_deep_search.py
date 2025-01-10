import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import logging
import csv
import time
import re

# Existing problems
# 1. response 202 is not handled
# 2. many websites which are working, are still missed inconsistently

# Idea
# run the current script on the input file, then create a browser based script to process the remaining websites. This should cover
# the cases such as 202 response and some other cases

logging.basicConfig(
    filename='vendor_scraper_deep_search.log',
    filemode='w',  # Overwrite the log file each run
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def make_request(url, tries=1):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
    }

    for attempt in range(tries):
        try:
            response = requests.get(url, timeout=30, headers=headers)
            response.raise_for_status()
            # if response.status_code != 200:
            #     logging.warning(f'Response code valid but not 200. Status code: {response.status_code}. URL: {url}')
            return response, response.status_code
        
        except UnicodeDecodeError as ude:
            logging.error(f"Unicode decode error for {url}: {ude}. Skipping this URL.")
            return None, None
        
        except requests.RequestException as e:
            logging.error(f"Error accessing {url} on attempt {attempt}: {e}")
            if e.response:
                return None, e.response.status_code
            if attempt < tries - 1:
                logging.info(f"Retrying {url} Attempt {attempt + 1}")
                time.sleep(5) 
            else:
                logging.error(f"Failed to access {url} after {tries} attempts.")
                return None, None  # None for status code if the request fails and no response is recieved


def ensure_scheme(url):
    if not url.startswith("http://") and not url.startswith("https://"):
        return 'http://' + url
    return url


def write_no_vendor_link(website):
    with open('no_vendor_links.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([website])
    return


def is_internal_link(base_url, link):
    """Checks if the link is an internal link of the base_url."""
    parsed_base = urlparse(base_url)
    parsed_link = urlparse(link)
    
    # An internal link either has the same domain or is a relative link
    return (parsed_link.netloc == '' or parsed_link.netloc == parsed_base.netloc)

def get_all_internal_links(base_url, soup):
    """Finds all internal links within the website homepage, excluding 'tel:' and 'mailto:' links."""
    links = set()
    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        # Exclude 'tel:' and 'mailto:' links
        if link.startswith(('tel:', 'mailto:', 'javascript:')) or re.search(r'\.(png|jpg|jpeg|pdf)$', link):
            continue 
        full_url = urljoin(base_url, link)
        if is_internal_link(base_url, full_url):
            links.add(full_url)
    return list(links)[:30]


def check_vendors_on_page(url, vendors):
    """Check a single page for competitor services."""

    response, _ = make_request(url, tries=2)
    if response and 'text/html' in response.headers.get('Content-Type', '').lower():
        try:  # ensure the response is parseable
            soup = BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logging.error(f"Error parsing HTML for {url}: {e}")
            return set()

        links = soup.find_all('a', href=True)
        vendors_found = set()

        for link in links:
            href = link['href'].lower()
            for vendor in vendors:
                if vendor in href:
                    vendors_found.add(vendor)

        page_source = response.text.lower()
        for vendor in vendors:
            if vendor in page_source:
                vendors_found.add(vendor)

        return vendors_found
    return set()


def process_website(lead_id, website, vendor_list):
    website_with_scheme = ensure_scheme(website)
    all_vendors = set() 
    response, status_code = make_request(website_with_scheme)
    
    # Check if the response is valid and if the response content is HTML
    if response and 'text/html' in response.headers.get('Content-Type', '').lower():
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Check the homepage for competitor services
        print(f"Checking homepage: {website_with_scheme}")
        vendors_found = check_vendors_on_page(website_with_scheme, vendor_list)
        if vendors_found:
            all_vendors.update(vendors_found)  # Add found vendors from homepage

        # Get all internal links from homepage
        internal_links = get_all_internal_links(website_with_scheme, soup)
        
        for link in internal_links:
            print(f"Checking internal link: {link}")
            vendors_found = check_vendors_on_page(link, vendor_list)
            if vendors_found:
                all_vendors.update(vendors_found)  # Add found vendors from internal links

    logging.info(f"website processed: {website}")
    
    # Return the result with all found vendors
    return {
        'lead_id': lead_id, 
        'website': website, 
        'vendors_found': ', '.join(all_vendors) if all_vendors else None, 
        'status_code': status_code
    }


def main():

    df = pd.read_excel('cv_rem.xlsx')

    websites = df[['lead_id', 'website']].astype(str)

    vendor_list = pd.read_csv('vendors.csv', encoding='ISO-8859-1')['Migration Form (Links)'].to_list()
    vendor_list = [str(vendor).lower() for vendor in vendor_list]

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_website, row['lead_id'], row['website'], vendor_list) for _, row in websites.iterrows()]
        processed_websites = [future.result() for future in futures]

    df = pd.DataFrame(processed_websites)
    df.to_excel('cv_rem_output.xlsx', index=False)


if __name__ == "__main__":
    start_time = datetime.now()
    main()
    end_time = datetime.now()
    print('Duration: {}'.format(end_time - start_time))




