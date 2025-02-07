import asyncio
import pandas as pd
from curl_cffi.requests import AsyncSession

async def check_url(session: AsyncSession, url: str):
    try:
        response = await session.get(url, impersonate="chrome110")
        return {
            'url': url,
            'status_code': response.status_code,
            'accessible': response.status_code == 200,
            'content_length': len(response.content)
        }
    except Exception as e:
        return {'url': url, 'accessible': False, 'error': str(e)}

async def validate_urls(urls: list[str]):
    async with AsyncSession() as session:
        tasks = [check_url(session, url) for url in urls]
        return await asyncio.gather(*tasks)

if __name__ == '__main__':
    df = pd.read_csv('websites.csv')
    urls = df['Website'].tolist()
    
    results = asyncio.run(validate_urls(urls))
    pd.DataFrame(results).to_csv('validation_results.csv', index=False)
    print(results)
    print("Results saved to validation_results.csv")


# NOTE: The urls that this script renders as invalid, are 100% invalid. The urls that are rendered as valid, are 98% valid.