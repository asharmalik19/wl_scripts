from playwright.sync_api import sync_playwright
import time

# search_string = 'site:*.janeapp.com -www'

def scrape_google_results():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto('https://www.google.com/search?q=site%3A*.janeapp.com+-www&sca_esv=4271166684a7579e&biw=1366&bih=633&sxsrf=AHTn8zptiOfg7y_MD7yC4uRE9KSCrDJv9Q%3A1738949938421&ei=MkWmZ562GYv8ptQP0L6pmAE&ved=0ahUKEwiel_KqjbKLAxULvokEHVBfChM4oAEQ4dUDCBA&uact=5&oq=site%3A*.janeapp.com+-www&gs_lp=Egxnd3Mtd2l6LXNlcnAiF3NpdGU6Ki5qYW5lYXBwLmNvbSAtd3d3SLwOUP4EWKoLcAF4AJABAJgB9AOgAdsGqgEHMy0xLjAuMbgBA8gBAPgBAZgCAKACAJgDAIgGAZIHAKAHWg&sclient=gws-wiz-serp')

        input('Press Enter after solving the captcha')
    
        links = []
        while True:
            results = page.query_selector_all('div.MjjYud a')
            for result in results:
                url = result.get_attribute('href')
                if 'janeapp.com' in url:
                    links.append(url)
                    print(url)
            
            next_button = page.query_selector('a#pnnext')
            if not next_button:
                break
                
            next_button.click()
            time.sleep(2)  # Wait for next page to load
            
        browser.close()
        return links

def main():
    scraped_links = scrape_google_results()
    
    # Print or process the results
    print(f"Found {len(scraped_links)} JaneApp domains:")
    with open('janeapp_domains.txt', 'w') as f:
        for link in scraped_links:
            f.write(link + '\n')

if __name__ == "__main__":
    main()



