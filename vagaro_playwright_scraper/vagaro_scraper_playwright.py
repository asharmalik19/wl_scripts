import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright

async def extract_website_link(page, url):
    try:
        await page.goto(url, timeout=50000)
        link_element = await page.query_selector('a.business-type-description.link')
        if link_element:
            href = await link_element.get_attribute('href')
            if href and href.startswith('http'):
                return href
        return None
    except Exception as e:
        print(f"Error extracting website link for {url}: {e}")
        return None

async def process_url(context, url, semaphore):
    async with semaphore:
        page = await context.new_page()
        website = await extract_website_link(page, url)
        await page.close()
        return {'VagaroURL': url, 'Website': website}

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
    merged_df.to_excel('joined_output.xlsx', index=False)
    return 

async def scrape_vagaro_websites(input_file, output_file):
    input_df = pd.read_excel(input_file)
    urls = input_df['VagaroURL'].dropna().tolist()
    
    async with async_playwright() as playwright:
        results = await process_urls(playwright, urls)
    
    df = pd.DataFrame(results)
    df.to_excel(output_file, index=False)

    join_dataframes(input_df, df)

if __name__ == '__main__':
    input_file = 'vagaro_medspa_3-6-25.xlsx'
    output_file = 'vagaro_medspa_3-6-25_output.xlsx'
    
    asyncio.run(scrape_vagaro_websites(input_file, output_file))
