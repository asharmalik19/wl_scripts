import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError
from bs4 import BeautifulSoup

async def get_business_hours(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    business_days = soup.find('div', id='divWorkingHoursDay').find_all('div')
    business_hours = soup.find('div', id='divWorkingHours').find_all('div')
    business_hours_dict = {}
    for day, hours in zip(business_days, business_hours):
        day_name = day.get_text(strip=True)
        hours_text = hours.get_text(strip=True)
        business_hours_dict[day_name] = hours_text
    return business_hours_dict

async def extract_link_and_hours(page, url):
    try:
        await page.goto(url, timeout=60000)
        business_days = page.locator('div#divWorkingHoursDay')
        await business_days.wait_for(state='attached', timeout=30000)
        page_source = await page.content()
        business_hours_and_link = await get_business_hours(page_source)
        link_element = await page.query_selector('a.business-type-description.link')
        if link_element:
            href = await link_element.get_attribute('href')
            if href and href.startswith('http'):
                business_hours_and_link.update({'Website': href})
        return business_hours_and_link
    except TimeoutError:
        print(f"Error extracting data for {url}: Timeout")
        return {}
    
async def process_url(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        link_and_hours = await extract_link_and_hours(page, url)
        await page.close()
        return {'VagaroURL': url, **link_and_hours}

async def process_urls(playwright, urls, max_concurrent_tasks=10):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    tasks = [process_url(context, url, semaphore) for url in urls]
    results = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing URLs"):
        result = await f
        results.append(result)
    
    await browser.close()
    return results

def join_dataframes(input_df, output_df):
    merged_df = pd.merge(input_df, output_df, on='VagaroURL', how='left')
    merged_df.to_csv('joined_output.csv', index=False)
    return 

async def scrape_vagaro_websites(input_file, output_file):
    input_df = pd.read_csv(input_file).iloc[:50]
    urls = input_df['VagaroURL'].dropna().tolist()
    
    async with async_playwright() as playwright:
        results = await process_urls(playwright, urls)
    
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)

    join_dataframes(input_df, df)

if __name__ == '__main__':
    input_file = 'vagaro-4_16_25.csv'
    output_file = 'vagaro-4_16_25-urls.csv'
    
    asyncio.run(scrape_vagaro_websites(input_file, output_file))
