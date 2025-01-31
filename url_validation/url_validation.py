import asyncio
import aiohttp
from typing import List

async def bulk_validate_urls(urls: List[str], timeout=10):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(asyncio.ensure_future(check_url(session, url, timeout)))
        return await asyncio.gather(*tasks)

async def check_url(session, url: str, timeout: int):
    try:
        async with session.get(url, timeout=timeout) as response:
            return {
                'url': url,
                'status_code': response.status,
                'accessible': response.status == 200,
                'content_length': len(await response.read())
            }
    except Exception as e:
        return {
            'url': url,
            'accessible': False,
            'error': str(e)
        }


if __name__ == '__main__':
    urls = [
        'http://www.heartandsolefzt.com/',
        'http://www.myluxemedspa.com/',
        'http://theglowsd.com/',
        'https://www.drmukerji.com/'
    ]

    results = asyncio.run(bulk_validate_urls(urls))
    print(results)
