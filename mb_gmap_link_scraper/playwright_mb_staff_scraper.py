import asyncio
import pandas as pd
from tqdm.asyncio import tqdm
from playwright.async_api import async_playwright
from playwright.async_api import TimeoutError, Error

async def extract_staff_and_maps(page, url):
    try:
        await page.goto(url, timeout=40000)
        staff_elements = await page.query_selector_all('h5.InstructorListItemDesktop_text__3jcsg')
        names = [await element.text_content() for element in staff_elements] if staff_elements else None

        maps_element = await page.query_selector('a.StudioAddress_link__3HM2W')
        maps_link = await maps_element.get_attribute('href') if maps_element else None
        return names, maps_link
    except TimeoutError:
        print(f"Timeout while loading {url}")
        return None, None
    except Error as e:
        print(f"Playwright error for {url}: {e}")
        return None, None

async def process_url(context, row, semaphore):
    async with semaphore:
        page = await context.new_page()
        names, maps_link = await extract_staff_and_maps(page, row['business_link'])
        await page.close()
        return {'id': row['id'], 'business_link': row['business_link'], 'staff_names': names, 'g_map_link': maps_link}

async def process_urls(playwright, data, max_concurrent_tasks=10):
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context()
    
    semaphore = asyncio.Semaphore(max_concurrent_tasks)
    tasks = [process_url(context, row, semaphore) for row in data.to_dict('records')]
    
    results = []
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing URLs"):
        result = await f
        results.append(result)
    
    await browser.close()
    return results

async def scrape_mindbody_data(input_file, output_file):
    input_df = pd.read_excel(input_file)
    
    async with async_playwright() as playwright:
        results = await process_urls(playwright, input_df)
    
    df = pd.DataFrame(results)
    df.to_csv(output_file, index=False)

if __name__ == '__main__':
    input_file = 'mb_data.xlsx'
    output_file = 'staff_names_and_links.csv'
    
    asyncio.run(scrape_mindbody_data(input_file, output_file))
