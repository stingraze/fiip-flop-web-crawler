#(C)Tsubasa Kato - Inspire Search Corporation - https://www.inspiresearch.io/en - 2024/8/16 17:47PM JST
#Created with help of Perplexity AI and ChatGPT (GPT-4o). Original algorithm IP by Tsubasa Kato
import urllib.parse
import time
import random
import sys
from collections import deque
import requests
from bs4 import BeautifulSoup
import argparse

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
                # Only include links to the same domain
                if urllib.parse.urlparse(full_url).netloc == urllib.parse.urlparse(url).netloc:
                    valid_links.append(full_url)
            
            return valid_links
        except requests.RequestException as e:
            print(f"Error fetching {url}: {e}")
            return []

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

                print(f"Using timing mode: Depth-first ratio {depth_ratio}, Breadth-first ratio {breadth_ratio}")

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
                    if current_depth + 1 <= self.max_depth:
                        child_urls = self.get_links(current_url)
                        for child_url in child_urls:
                            if child_url not in self.visited and all(child_url != url for url, _ in queue) and all(child_url != url for url, _ in stack):
                                queue.append((child_url, current_depth + 1))
                                stack.append((child_url, current_depth + 1))

                timing_index += 1
        except KeyboardInterrupt:
            print("\nCrawling stopped by user. Exiting gracefully...")

# Argument parsing
def main():
    parser = argparse.ArgumentParser(description="Hybrid Web Crawler using DFS and BFS.")
    parser.add_argument('url', type=str, help="The starting URL to crawl.")
    parser.add_argument('max_depth', type=int, help="Maximum depth to crawl.")

    args = parser.parse_args()

    crawler = HybridWebCrawler(args.url, args.max_depth)
    crawler.crawl()

if __name__ == "__main__":
    main()
