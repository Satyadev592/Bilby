from bs4 import BeautifulSoup
import anthropic
import traceback
import datetime
import requests
import redis
import json
import re

redis_conn = redis.Redis()
# Base Class for scraping, can inherit this across different scrapers
class Scraper:
    def __init__(self, url)->None:
        self.soup = None
        self.url = url

    def get_data(self)->None:
        req = requests.get(self.url, headers={
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36"
        })
        self.soup = BeautifulSoup(req.content, "html.parser")

    def extract_data(self):
        # Implement this per inherited class
        raise NotImplementedError

# Class for scraping data specific to nasdaq assuming all blogs on nasdaq have a similar pattern
# This class can be moved into a separate file in future and one class per website/pattern can be maintained
class Nasdaq(Scraper):

    def __init__(self, url) ->None:
        super().__init__(url)
        # Define all the unique attributes needed for extraction
        self.title = ''
        self.publisher = ''
        self.published_timestamp= ''
        self.author= ''
        self.topics = ''
        self.body_content= ''
        self.companies = ''

    def extract_data(self) -> None:
        #Function to extract article data and metadata
        self.title = self.soup.body.h1.getText().strip()
        #We are filtering for the div relevant for extracting metadata
        author_details_soup = self.soup.find("div", class_="jupiter22-c-author-byline")
        self.author = author_details_soup.find(attrs={"class": "jupiter22-c-author-byline__author-no-link"}).getText()
        self.published_timestamp = author_details_soup.find(attrs={"class": "jupiter22-c-author-byline__timestamp"}).getText()
        self.publisher = re.sub("[^A-Za-z0-9 ]+", '',
                           author_details_soup.find(attrs={"class": "jupiter22-c-text-link__text"}).getText())

        body = self.soup.find("div", class_="body").find_all('p')
        self.body_content = ''
        for paragraph in body:
            self.body_content += paragraph.getText()

        topics_extracted = self.soup.find("div", class_="body").find_all('h2')
        self.topics = []
        for topic in topics_extracted:
            self.topics.append(topic.getText())

    def extract_stock_names(self)->None:
        # Using an LLm here for extracting stock names, we can do this without LLM but that would be maintenance heavy
        client = anthropic.Anthropic(
            api_key="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",
        )
        message = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=1024,
            messages=[
                {"role": "user",
                 "content": "Please extract company/stock names from the following text, do not include any ETF: ```{0}```".format(
                     self.body_content)}
            ]
        )
        company_names = [i for i in message.content[0].text.split('\n') if i and i[0].isdigit()]
        self.companies = [i.split(". ")[1] for i in company_names]

    def prepare_data_for_registry(self):
        data = {'url': self.url, 'title': self.title, 'author': self.author, 'publisher': self.publisher,
                'publication_timestamp': self.published_timestamp, 'stocks': self.companies,
                'datetime_of_extraction': str(datetime.datetime.now())}
        return data


def insert_into_registry(data, key):
    # Using Redis as a registry here, ideally MongoDB or a NOSQL DB is what we need, redis is a cache, and I have
    # used it as it's simple to set up, in production I might use mongoDB
    try:
        redis_conn.set(key, json.dumps(data))
    except:
        print("Unable to insert data - please check")
        print(traceback.format_exc())
    return

def read_from_registry(key):
    try:
        data = json.loads(redis_conn.get(key))
        return data
    except:
        print("Url does not exist or data wrongly inserted into redis")
        print(traceback.format_exc())

if __name__=="__main__":
    url = "https://www.nasdaq.com/articles/should-investors-buy-the-artificial-intelligence-technology-etf-instead-of-individual-ai"
    scraper = Nasdaq(url)
    scraper.get_data()
    scraper.extract_data()
    scraper.extract_stock_names()
    data_for_registry = scraper.prepare_data_for_registry()
    insert_into_registry(data_for_registry, url)
    print(read_from_registry(url))