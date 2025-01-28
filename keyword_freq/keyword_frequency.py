import asyncio
import pandas as pd
from bs4 import BeautifulSoup
import logging
from curl_cffi.requests import AsyncSession

logging.basicConfig(
    filename='website_errors.log',
    level=logging.ERROR,
    filemode='w',
    format='%(asctime)s - %(message)s'
)

async def fetch_url(session, url):
    try:
        response = await session.get(url, timeout=30, impersonate="chrome110")
        if response.status_code != 200:
            logging.error(f"Failed to fetch {url} - Status code: {response.status_code}")
            return None
        return response.text
    except Exception as e:
        logging.error(f"Error fetching {url}: {str(e)}")
        return None

async def get_keyword_frequency(session, id, url, keywords):
    print(f"Processing: {url}")
    html_content = await fetch_url(session, url)
    if html_content is None:
        return [id, url] + [-1] * len(keywords)
    
    soup = BeautifulSoup(html_content, 'html.parser')
    text_content = soup.get_text().lower()
    
    frequencies = [text_content.count(keyword.lower()) for keyword in keywords]
    return [id, url] + frequencies

async def main():
    df = pd.read_csv('websites.csv')
    print(df)
    keywords = input("Enter keywords (comma-separated): ").split(',')
    
    results = []
    async with AsyncSession() as session:
        tasks = [get_keyword_frequency(session, row['ID'], row['Website'], keywords) 
                for _, row in df.iterrows()]
        results = await asyncio.gather(*tasks)
    
    columns = ['ID', 'Website'] + keywords
    output_df = pd.DataFrame(results, columns=columns)
    output_df.to_csv('output.csv', index=False)

if __name__ == "__main__":
    asyncio.run(main())
