import os
import csv
import urllib.parse
import time
import random
import requests
from bs4 import BeautifulSoup
from collections import deque
import sys

class HybridWebCrawler:
    def __init__(self, start_url, max_depth):
        self.start_url = start_url
        self.max_depth = max_depth
        self.visited = set()

    def visit(self, url, mode, depth):
        if url not in self.visited:
            print(f'{mode} visiting {url} (depth: {depth})')
            self.visited.add(url)
            time.sleep(1)  # Delay to be respectful to the server
            return True
        return False

    def get_links(self, url):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            links = soup.find_all('a', href=True)
            
            valid_links = []
            for link in links:
                href = link['href']
                full_url = urllib.parse.urljoin(url, href)
                if urllib.parse.urlparse(full_url).netloc == urllib.parse.urlparse(url).netloc:
                    valid_links.append(full_url)
            return valid_links
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return []

    def scrape_page(self, url):
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Scrape title, meta keywords, meta description, and body content
            title = soup.title.string if soup.title else 'No Title'
            keywords = soup.find('meta', attrs={'name': 'keywords'})
            description = soup.find('meta', attrs={'name': 'description'})
            keywords_content = keywords['content'] if keywords else 'No Keywords'
            description_content = description['content'] if description else 'No Description'
            body_content = soup.get_text()  # Get the text from the body of the page

            # Clean the body content
            cleaned_body = self.clean_body_content(body_content)

            return { 'url': url, 'title': title, 'keywords': keywords_content, 'description': description_content, 'body': cleaned_body }
        except Exception as e:
            print(f'Error scraping {url}: {e}')
            return None

    def clean_body_content(self, body):
        # Remove line breaks and extra whitespace
        cleaned_body = ' '.join(body.split())
        # Shorten the body to 800 characters
        return cleaned_body[:800]

    def save_to_csv(self, data, filename='scraped_data.csv'):
        file_exists = os.path.isfile(filename)
        try:
            with open(filename, mode='a', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=['url', 'title', 'keywords', 'description', 'body'])
                if not file_exists:
                    writer.writeheader()  # Write header only if file does not exist
                writer.writerow(data)
        except Exception as e:
            print(f"Error saving to CSV: {e}")

    def crawl(self):
        queue = deque([(self.start_url, 0)])  # BFS queue
        stack = [(self.start_url, 0)]  # DFS stack
        timing_modes = [(3, 2), (3, 1), (4, 1)]
        timing_index = 0

        try:
            while queue or stack:
                depth_ratio, breadth_ratio = timing_modes[timing_index % len(timing_modes)]
                total_ratio = depth_ratio + breadth_ratio
                choice = random.randint(1, total_ratio)

                if choice <= depth_ratio and stack:
                    current_url, current_depth = stack.pop()  # DFS
                    mode = "Depth-first"
                else:
                    if queue:
                        current_url, current_depth = queue.popleft()  # BFS
                        mode = "Breadth-first"
                    else:
                        continue

                if current_depth > self.max_depth:
                    continue

                if self.visit(current_url, mode, current_depth):
                    scraped_data = self.scrape_page(current_url)
                    if scraped_data:
                        self.save_to_csv(scraped_data)
                    if current_depth + 1 <= self.max_depth:
                        child_urls = self.get_links(current_url)
                        for child_url in child_urls:
                            if child_url not in self.visited and all(child_url != url for url, _ in queue) and all(child_url != url for url, _ in stack):
                                queue.append((child_url, current_depth + 1))
                                stack.append((child_url, current_depth + 1))

                timing_index += 1
        except KeyboardInterrupt:
            print("\nCrawling stopped by user. Exiting gracefully...")

if __name__ == '__main__':
    start_url = sys.argv[1]
    max_depth = int(sys.argv[2])
    crawler = HybridWebCrawler(start_url, max_depth)
    crawler.crawl()
