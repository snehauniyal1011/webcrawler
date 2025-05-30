import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin, urlparse
from tenacity import retry, stop_after_attempt, wait_exponential
from queue import Queue
import csv
import time
from urllib.robotparser import RobotFileParser
from datetime import datetime
import re 

TARGET_URL = "http://books.toscrape.com"
MAX_CRAWL = 5
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
DELAY = 1
KEYWORDS = []

visited_urls = set()
all_pages = set()  
all_images = set()  
all_links_found = set()  
keyword_matches = {} 

high_priority_queue = Queue()
low_priority_queue = Queue()
high_priority_queue.put(TARGET_URL)

session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})

rp = RobotFileParser()

def load_robots_txt(base_url):
    robots_url = urljoin(base_url, '/robots.txt')
    rp.set_url(robots_url)
    try:
        rp.read()
    except Exception as e:
        print(f"Couldn't read robots.txt: {e}")
    return rp

def can_fetch(url):
    return rp.can_fetch(USER_AGENT, url)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def fetch_url(url):
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {e}")
        raise

def is_same_domain(url, base_url):
    return urlparse(url).netloc == urlparse(base_url).netloc

def normalize_url(url):
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

def check_keywords(content):
    content_lower = content.lower()
    return [kw for kw in KEYWORDS if kw.lower() in content_lower]

def extract_links_and_images(soup, base_url):
    links = set()
    images = set()
    
    for link in soup.find_all('a', href=True):
        url = link['href'].strip()
        if url and not url.startswith(('javascript:', '#')):
            absolute_url = urljoin(base_url, url)
            absolute_url = normalize_url(absolute_url)
            if is_same_domain(absolute_url, base_url):
                link_text = link.get_text(strip=True) or "No link text"
                links.add((absolute_url, link_text))
    
    for img in soup.find_all('img', src=True):
        img_url = img['src'].strip()
        if img_url:
            absolute_img_url = urljoin(base_url, img_url)
            images.add(absolute_img_url)
    
    return links, images

def crawl():
    if not TARGET_URL:
        print("No target URL set for crawling.")
        return
    
    load_robots_txt(TARGET_URL)
    crawl_count = 0
    
    while (not high_priority_queue.empty() or not low_priority_queue.empty()) and crawl_count < MAX_CRAWL:
        current_url = high_priority_queue.get() if not high_priority_queue.empty() else low_priority_queue.get()
        
        if current_url in visited_urls:
            continue

        print(f"Crawling {current_url} ({crawl_count + 1}/{MAX_CRAWL})")
        
        try:
            response = fetch_url(current_url)
            visited_urls.add(current_url)
            
            timestamp = datetime.now().isoformat()
            soup = BeautifulSoup(response.content, 'html.parser')
            page_title = soup.title.string.strip() if soup.title else "No Title"
            
            all_pages.add((current_url, page_title, timestamp))
            
            for img in soup.find_all('img', src=True):
                img_url = urljoin(current_url, img['src'])
                img_alt = img.get('alt', 'No alt text')
                all_images.add((img_url, img_alt, page_title, current_url, timestamp))
            
            new_links, new_images = extract_links_and_images(soup, current_url)
            for link_url, link_text in new_links:
                all_links_found.add((link_url, link_text, page_title, current_url, timestamp))
            
            text_content = soup.get_text()
            matched_keywords = check_keywords(text_content)
            if matched_keywords:
                keyword_matches[current_url] = {
                    'title': page_title,
                    'keywords': matched_keywords,
                    'timestamp': timestamp
                }
                print(f"Found keywords {matched_keywords} in {current_url}")
            
            for link in new_links:
                if link[0] not in visited_urls and not any(link[0] == url for url, _, _ in all_pages):
                    if any(re.search(rf'/{kw}/', link[0], re.I) for kw in KEYWORDS):
                        high_priority_queue.put(link[0])
                    else:
                        low_priority_queue.put(link[0])
            
            time.sleep(DELAY)
            crawl_count += 1
            
        except Exception as e:
            print(f"Failed to process {current_url}: {str(e)}")
            continue

def save_results():
    os.makedirs("crawler_output", exist_ok=True)
    
    with open(os.path.join("crawler_output", 'pages.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Title', 'URL', 'Timestamp'])
        for url, title, timestamp in sorted(all_pages, key=lambda x: x[2], reverse=True):
            writer.writerow([title, url, timestamp])
    
    with open(os.path.join("crawler_output", 'images.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Image URL', 'Alt Text', 'Page Title', 'Source URL', 'Timestamp', 'File Type'])
        for img_url, alt, title, src_url, timestamp in sorted(all_images, key=lambda x: x[4], reverse=True):
            file_type = os.path.splitext(img_url)[1][1:].upper() or "UNKNOWN"
            writer.writerow([img_url, alt, title, src_url, timestamp, file_type])
    
    with open(os.path.join("crawler_output", 'all_links.csv'), 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Link URL', 'Link Text', 'Page Title', 'Source URL', 'Timestamp'])
        for link_url, link_text, page_title, source_url, timestamp in sorted(all_links_found, key=lambda x: x[4], reverse=True):
            writer.writerow([link_url, link_text, page_title, source_url, timestamp])
    
    if keyword_matches:
        with open(os.path.join("crawler_output", 'keyword_matches.csv'), 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Page Title', 'URL', 'Keywords', 'Timestamp'])
            for url, data in keyword_matches.items():
                writer.writerow([data['title'], url, ', '.join(data['keywords']), data['timestamp']])
