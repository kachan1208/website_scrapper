from page import Page
from urllib.parse import urlparse 
from urllib.parse import urlunparse
import re
import sys
import asyncio
import aiohttp

from time import sleep

class Spider:
    _history = []    
    _queue = []
    _headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:52.0) Gecko/20100101 Firefox/52.0",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive"
    } 
    
    _not_allowed_formats = [
        'pdf',
        'jpg',
        'jpeg',
        'png',  
        'gif',
        'ico'
    ]

    def __init__(self, url):
        self._host = self.get_host(url)
        self._scheme = self.get_scheme(url)
        self.enqueue(url)
        self._create_session()
        self._max_threads_amount = 16                   
        self._threads_amount = 0

    def _create_session(self):
        self._connector = aiohttp.TCPConnector() #limit=self._max_threads_amount)
        self._session = aiohttp.ClientSession(connector=self._connector, headers=self._headers) 

    def enqueue(self, url):
        is_same_host = self.is_same_host(url)
        url = self.clean_url(url)
        is_in_history = self.is_in_history(url)
        is_in_queue = url in self._queue
        is_allowed_format = self.is_allowed_format(url)

        if is_same_host and is_allowed_format and not is_in_queue and not is_in_history:
            self._queue.append(url)

    async def run(self):
        while await self._is_processing_allowed():
            url = self.dequeue()
            future = asyncio.ensure_future(self.process(url))
            future.add_done_callback(self.done_process)
            self._threads_amount += 1

            while self._threads_amount >= self._max_threads_amount:
                await asyncio.sleep(0.01)

    async def process(self, url):
        self.save_history(url)

        page = await self.request(url)
        await page.read_content()

        urls = page.get_links(self._host)
        for new_url in urls:
            self.enqueue(new_url)

        return url

    def done_process(self, future):
        result = future.result()
        self._threads_amount -= 1
        print('done')
        print(result)

    async def request(self, url):
        url = self._scheme + '://' + url
        response = await self._session.get(url)
        return Page(response)

    def process_page(self, page):
        print(page.html)

    def clean_url(self, url):
        url = re.sub('^(https?:)?//', '', url) #remove scheme or '//' from the begining
        if url[-1] == '/':
            url = url[:-1]
        
        return url

    def get_host(self, url):
        return urlparse(url).netloc.strip('www.')

    def get_scheme(self, url):
        return urlparse(url).scheme

    def dequeue(self):
        url = self._queue.pop()
        
        return url

    def save_history(self, url):
        self._history.append(url)

        return url

    def is_in_history(self, url):
        return url in self._history

    def is_same_host(self, url):
        return self.get_host(url) == self._host

    def is_empty_queue(self):
        return not self._queue

    def is_allowed_format(self, url):
        return url.split('.')[-1] not in self._not_allowed_formats

    async def _is_processing_allowed(self):
        result = True
        while self._threads_amount > 0 and self.is_empty_queue():
            await asyncio.sleep(0.01)

        if self.is_empty_queue():
            result = False

        return result


if __name__ == '__main__':
    # requests_cache.install_cache('spider')
    loop = asyncio.get_event_loop()
    spider = Spider('http://ciklum.com')
    loop.run_until_complete(spider.run())