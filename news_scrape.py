import csv
from datetime import datetime, timedelta
import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, Join

class Article(scrapy.Item):
    date = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()

class Spider1(scrapy.Spider):

    name = "spider-1"
    
    def generate_url(self, site_url, date, date_format, page_number=None, page_format=None):
        url = ''
        if date_format == "year_month_day":
            url = site_url + date.strftime("%Y/%m/%d") + "/"
        elif date_format == "year_month":
            url = site_url + date.strftime("%Y/%m") + "/"
        elif date_format == "year":
            url = site_url + date.strftime("%Y") + "/"
        if page_number:
            if page_format == "slash":
                url += "page/" + str(page_number) + "/"
            elif page_format == "question":
                url += "?page=" + str(page_number) + "/"
            elif page_format == "question_number":
                url += "?" + str(page_number) + "/"
        return url

    def start_requests(self):
        start_date = datetime(year=2015, month=1, day=1)
        with open('archive_idents.csv', 'r') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                date = start_date
                while date <= datetime.now():
                    site_url = row['site_url']
                    date_format = row['date_format']
                    url = self.generate_url(site_url=site_url, date=date, date_format=date_format)
                    new_request = scrapy.Request(url)
                    new_request.meta["site_url"] = site_url
                    new_request.meta["date"] = date
                    new_request.meta["page_number"] = 1
                    new_request.meta["article_link"] = row['article_link']
                    new_request.meta["title"] = row['title']
                    new_request.meta["content"] = row['content']
                    new_request.meta["date_format"] = date_format
                    new_request.meta["page_format"] = row['page_format']
                    yield new_request
                    if date_format == "year_month_day":
                        date += timedelta(days=1)
                    elif date_format == "year_month":
                        date += timedelta(days=30)
                    elif date_format == "year":
                        date += timedelta(days=365)
    
    def parse(self, response):
        date = response.meta['date']
        page_number = response.meta['page_number']
        date_format = response.meta['date_format']
        page_format = response.meta['page_format']
        articles = response.xpath(response.meta['article_link']).extract()
        for url in articles:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            request = scrapy.Request(url, headers=headers, callback=self.parse_article)
            request.meta['date'] = date
            request.meta['article_link'] = response.meta['article_link']
            request.meta['title'] = response.meta['title']
            request.meta['content'] = response.meta['content']
            request.meta['page_number'] = page_number
            request.meta["date_format"] = date_format
            request.meta["page_format"] = page_format
            request.meta['site_url'] = response.meta['site_url']
            yield request
        next_page_number = page_number + 1
        next_page_url = self.generate_url(site_url= response.meta['site_url'], date=date, page_number=next_page_number, date_format=date_format, page_format=page_format)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        request = scrapy.Request(next_page_url, headers=headers, callback=self.parse)
        request.meta['date'] = date
        request.meta['page_number'] = next_page_number
        request.meta['date_format'] = date_format
        request.meta["page_format"] = page_format
        request.meta['article_link'] = response.meta['article_link']
        request.meta['title'] = response.meta['title']
        request.meta['content'] = response.meta['content']
        request.meta['site_url'] = response.meta['site_url']
        yield request

    def parse_article(self, response):
        l = Spider1ArticleLoader(Article(), response=response)
        l.add_value('date', response.meta['date'].strftime("%Y-%m-%d"))
        l.add_value('url', response.url)
        l.add_xpath('title', response.meta['title'])
        l.add_xpath('content', response.meta['content'])
        return l.load_item()

class Spider1ArticleLoader(ItemLoader):
    default_input_processor = MapCompose(str.strip)
    default_output_processor = Join()

    tags_in = MapCompose(str.strip)
    tags_out = Join(separator=u'; ')
    