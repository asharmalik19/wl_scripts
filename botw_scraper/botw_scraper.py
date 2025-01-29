from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
from selenium.webdriver.common.by import By
import requests
import pandas as pd
import undetected_chromedriver as uc
import logging

logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    filemode='w',
    format='%(asctime)s - %(message)s'
)

def setup_driver():
    driver = uc.Chrome(headless=False,use_subprocess=False)
    return driver

def get_business_details(soup):
    name = soup.select_one('h1.mt-0')
    sidebar = soup.select_one('div.sidebar')
    address = sidebar.select_one('li.list-group-item i.bi-geo-alt + a')
    g_map_link = address['href'] if address else None
    phone = sidebar.select_one('li.list-group-item i.bi-telephone + a')
    category = sidebar.select_one('div.business_categories a.btn-primary')

    return {
        'name': name.text.strip() if name else None,
        'address': address.text.strip() if address else None,
        'phone': phone.text if phone else None,
        'category': category.text if category else None,
        'google_map_link': g_map_link
    }

def get_business_links(driver, keyword):
    search_url = f'https://botw.org/search/?q={keyword}'
    driver.get(search_url)
    input('captcha solved?')  # manually solve the captcha
    all_links = []
    
    while True:
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        results = soup.select("div.gsc-webResult.gsc-result")
        for result in results:
            first_link = result.select_one("a")
            if first_link and first_link.get('href'):
                all_links.append(first_link['href'])
        
        current_page = soup.select_one("div.gsc-cursor-current-page")
        if current_page.text == "10":
            break
        next_page_num = int(current_page.text) + 1
        driver.execute_script("arguments[0].click();", driver.find_element(By.CSS_SELECTOR, f"div.gsc-cursor-page[aria-label='Page {next_page_num}']"))
        time.sleep(1)
    print(f'links {all_links}')
    return all_links

def scrape_businesses(keyword='gym'):
    driver = setup_driver()
    businesses = []
    business_links = get_business_links(driver, keyword)
    driver.quit()
    for link in business_links:
        if '/listing/' not in link:
            logging.info(f"not a business page {link}")
            continue
        try:
            response = requests.get(link)
        except Exception:
            print(f"Failed to fetch {link}")
            logging.error(f"Failed to fetch {link}")
            continue
        soup = BeautifulSoup(response.content, 'html.parser')
        business = get_business_details(soup)
        business['business_link'] = link
        businesses.append(business)
        print(f"Scraped: {business['name']}") 
    return businesses

def main():
    businesses = scrape_businesses()
    df = pd.DataFrame(businesses)
    df.to_csv('botw_businesses.csv', index=False)

if __name__ == '__main__':
    main()