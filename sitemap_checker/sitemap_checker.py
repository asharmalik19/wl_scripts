import pandas as pd
import asyncio
import aiohttp
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

async def extract_urls_from_sitemap(session, sitemap_url):
    try:
        async with session.get(sitemap_url, timeout=10) as response:
            content = await response.text()
            root = ET.fromstring(content)
            
            urls = []
            for elem in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
                url = elem.text
                if url.endswith('.xml'):
                    urls.extend(await extract_urls_from_sitemap(session, url))
                else:
                    urls.append(url.lower())
            return urls
    except Exception as e:
        print(f"Error parsing sitemap {sitemap_url}: {str(e)}")
        return []

async def find_pages(session, url):
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    services_url = ''
    schedule_url = ''
    location_url = ''
    
    try:
        sitemap_url = ''
        
        # Try robots.txt
        robots_url = urljoin(url, '/robots.txt')
        async with session.get(robots_url, timeout=10) as response:
            if response.status == 200:
                robots_text = await response.text()
                for line in robots_text.split('\n'):
                    if 'sitemap:' in line.lower():
                        sitemap_url = line.split(':', 1)[1].strip()
                        break
        
        # Try common sitemap locations if not found in robots.txt
        if not sitemap_url:
            common_paths = ['/sitemap.xml', '/sitemap_index.xml', '/sitemap-index.xml']
            for path in common_paths:
                test_url = urljoin(url, path)
                try:
                    async with session.get(test_url, timeout=10) as response:
                        if response.status == 200:
                            content = await response.text()
                            ET.fromstring(content)
                            sitemap_url = test_url
                            break
                except Exception:
                    continue
        
        if sitemap_url:
            urls = await extract_urls_from_sitemap(session, sitemap_url)
            
            for page_url in urls:
                if '/service' in page_url:
                    services_url = page_url
                elif '/schedule' in page_url or '/booking' in page_url or '/appointment' in page_url:
                    schedule_url = page_url
                elif '/location' in page_url or '/stores' in page_url or '/branches' in page_url:
                    location_url = page_url
                    
    except Exception as e:
        print(f"Error: {url} - {str(e)}")
    
    return {'website': url, 'services': services_url, 'schedule': schedule_url, 'location': location_url}

async def process_websites(websites):
    async with aiohttp.ClientSession() as session:
        tasks = [find_pages(session, website) for website in websites]
        return await asyncio.gather(*tasks)

def main():
    print("Reading Excel file...")
    df = pd.read_excel('cv.xlsx').iloc[:100]
    
    print(f"Processing {len(df)} websites...")
    results = asyncio.run(process_websites(df['website']))
    results_df = pd.DataFrame(results)
    
    pages_found = {
        'services': results_df['services'].str.len().gt(0).sum(),
        'schedule': results_df['schedule'].str.len().gt(0).sum(),
        'location': results_df['location'].str.len().gt(0).sum()
    }
    
    print("\nResults:")
    for page_type, count in pages_found.items():
        print(f"Found {count} {page_type} pages")
    
    results_df.to_csv('page_results.csv', index=False)
    print("Results saved to page_results.csv")

if __name__ == "__main__":
    main()