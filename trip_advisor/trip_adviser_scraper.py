import undetected_chromedriver as uc 
import time
from selenium.webdriver.common.by import By
import re
import pandas as pd
from datetime import datetime
import time
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import random
from selenium.common.exceptions import TimeoutException

def get_links(soup, BASE_URL):
    articles = soup.select('article.GTuVU.XJlaI')
    busieness_links = [BASE_URL + article.find('a')['href'] for article in articles]
    return busieness_links


def scrape_business_details(soup):
    details = {
        'business_title': '',
        'address': '',
        'phone_number': '',
        'email': '',
        'website': ''
    }
    # Business title
    title = soup.select_one('h1.biGQs._P.fiohW.eIegw')
    if title:
        details['business_title'] = title.text.strip()

    # Address
    address = soup.select_one('div.MJ span')  # note: this selector matches many elements but the first match is the address
    if address:
        details['address'] = address.text.strip()

    # Contact details (website link, phone number, email)
    contact_info = soup.select('div.DXZlm.Q3.K.ML a')
    for info in contact_info:
        href = info.get('href')
        if 'mailto:' in href:
            details['email'] = href.replace('mailto:', '').strip()
        elif 'tel:' in href:
            phone_number = href.replace('tel:', '').strip()
            cleaned_phone_number = phone_number.replace('%2B1%20', '')
            details['phone_number'] = cleaned_phone_number
        else:
            details['website'] = href.strip()

    return details


def append_to_csv(businesses_info, URL):
    df = pd.DataFrame(businesses_info)
    df['url'] = URL
    df.to_csv('trip_adviser.csv', mode='a', index=False, header=False)
    return


def main():
    URL = 'https://www.tripadvisor.com/Attractions-g191-Activities-c40-t129-oa1260-United_States.html'
    BASE_URL = 'https://www.tripadvisor.com'
    brave_path = r'C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe'
    driver = uc.Chrome(browser_executable_path=brave_path, headless=False)
    driver.maximize_window()

    while True:
        driver.get(URL)
        time.sleep(random.uniform(2, 3))

        # todo: we need to check if the captcha is dispalyed or not

        main_page_source = driver.page_source
        main_page_soup = BeautifulSoup(main_page_source, 'html.parser')
        business_links = get_links(main_page_soup, BASE_URL)

        businesses_info = []
        for link in business_links:
            driver.get(link)
            time.sleep(random.uniform(1, 3))

            # wait for a while and check if the contact info becomes available
            try:
                WebDriverWait(driver, 2).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.DXZlm.Q3.K.ML a')))
            except TimeoutException:
                print("Timed out waiting for contact info to become available")           
            business_page_source = driver.page_source
            business_page_soup = BeautifulSoup(business_page_source, 'html.parser')
            details = scrape_business_details(business_page_soup)
            print(details)
            print('----------------------------------------')
            businesses_info.append(details)

        append_to_csv(businesses_info, URL)

        # Navigate back to the business listings page using URL
        driver.get(URL)

        # Find the next page link and navigate
        try:
            next_page_element = driver.find_element(By.CSS_SELECTOR, 'a[aria-label="Next page"]')
            next_page_link = next_page_element.get_attribute('href')
            URL = next_page_link
            print(URL)
        except Exception as e:
            print("No more pages to scrape.")
            driver.save_screenshot('last_page.png')
            break

    driver.quit()
    

if __name__ == '__main__':
    main()

