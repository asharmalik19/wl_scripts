import asyncio
from curl_cffi.requests import AsyncSession
import html_text
import tiktoken
import pandas as pd

def get_token_count(content):
    encoding = tiktoken.get_encoding("o200k_base")
    token_count = len(encoding.encode(content))
    print(f'token_count: {token_count}')
    return token_count

async def scrape_url(session, url):
    try:
        response = await session.get(url)
        if response.status_code == 200:
            content = html_text.extract_text(response.text)
            print(f"\nContent from {url}:\n{content[:500]}...")  # Print first 500 chars
            return {
                'url': url,
                'content': content,  # Added content to output
                'content_length': len(content),
                'token_count': get_token_count(content)
            }
    except Exception:
        return {
            'url': url,
            'content': '',  # Empty content for failed requests
            'content_length': 0, 
            'token_count': 0
        }
    return {
        'url': url,
        'content': '',
        'content_length': 0,
        'token_count': 0
    }

async def process_urls(urls):
    async with AsyncSession(max_clients=10) as session:
        # tasks = [scrape_url(session, url) for url in urls]
        # return await asyncio.gather(*tasks)
        all_info = []
        for url in urls:
            response = await session.get(url)
            content = html_text.extract_text(response.text)
            content_length = len(content)
            token_count = get_token_count(content)

            info = {
                'url': url,
                'content': content,
                'content_length': content_length,
                'token_count': token_count
            }
            all_info.append(info)
    return all_info



if __name__ == '__main__':
    urls = pd.read_csv('bug_urls.csv')['url'].tolist()[:100]
    results = asyncio.run(process_urls(urls))

    results_df = pd.DataFrame(results)
    results_df.to_csv('scraping_results.csv', index=False)
    print(f"URLs processed: {len(results_df)}")

