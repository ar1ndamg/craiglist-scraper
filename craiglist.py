# -*- coding: utf-8 -*-
"""
Craiglist Spider using scrapy.
Test search URLs:
    # https://chicago.craigslist.org/search/jjj?query=sales
    # https://detroit.craigslist.org/search/jjj?query=sales

This spider takes list of search URLs from 'input.txt' and scrapes details for every item found in seacrh.
"""

import os
import random
import datetime
from time import sleep,time
import sys

import scrapy
from scrapy import Request

class CraiglistSpider(scrapy.Spider):
    name = 'craiglist'
    #allowed_domains = ['craigslist.org']
    start_urls = ['https://tampa.craigslist.org/search/jjj?query=bookkeeper']

    def __init__(self):
        self.session_id = 0
        self.counter = 1
        if os.path.exists('./sessions.txt'):
            with open('./sessions.txt', 'r') as session_file:
                self.session_id = int(session_file.readline())
        self.session_id+=1
        with open('./sessions.txt', 'w') as session_file:
            session_file.write(str(self.session_id))
        self.start = time()


    def parse(self, response):
        """ read urls from a file and yield GET request """
        with open('./input.txt','r')  as url_list:
            for url in url_list:
                url = url.strip('\n')
                if url:
                    print('Getting URL: {}'.format(url))
                    sleep(random.randrange(5,10))
                    yield Request(url, callback=self.fetch_url, meta={'url': url})

    def fetch_url(self, response):
        """ Scrapes all the post links in the search results page 
            Also stores the already scraped URLs in a file to remove duplication        
        """
        search_url = response.meta['url']     
        links = response.xpath('//*[@class="result-title hdrlnk"]/@href').extract()
        print('{} Links found on this page.'.format(len(links)))
        urls = []
        try:
            with open("./completed_urls.txt",'r') as completed_urls:
                urls = completed_urls.readlines()
        except:
            print("No previously saved  urls found! So, a new file will be created now.")
            f = open("./completed_urls.txt",'w')
            f.close()
        for link in links:
            if link+'\n' not in urls:
                sleep(random.randrange(5,10))
                yield Request(link, callback=self.get_details, meta={'url': search_url} )
        relative_next_url = response.xpath('//a[@class="button next"]/@href').extract_first()
        if relative_next_url:
            absolute_next_url = response.url.split("/search")[0] + relative_next_url
            print(absolute_next_url)
            sleep(random.randrange(5,10))
            yield Request(absolute_next_url, callback=self.fetch_url, meta={'url': search_url})   

    def get_details(self, response):
        """ Scrapes the required details """
        print("Pages scrapped: {}".format(self.counter))
        print ("User Agent: {}".format(response.request.headers['User-Agent']))
        try:
            print ("Proxy: {}".format(response.request.meta['proxy']))
        except:
            print("No proxy in meta.")
        print("Getting details of the post {}".format(response.url))
        link = response.url
        # post id
        post_id = link.split('.html')[0].split('/')[-1]
        print("Post ID: {}".format(post_id))
        #title
        titles = response.css('.postingtitletext *::text').extract()
        title=''
        for item in titles:
            title+=item.strip('\n')
        title = title.strip()
        body_text = "".join(line for line in response.xpath('//*[@id="postingbody"]//text()[not(ancestor::div/@class="print-information print-qrcode-container")]').extract()).strip()
        notice_list = "".join(line.strip()+"\n" for line in response.css('.notices *::text').extract()).strip()
        attributes = response.xpath('//p[@class="attrgroup"]')
        attr_text=''
        # post attributes
        for attribute in attributes:
            attr_text = ' '.join(t.strip()+'\n' for t in attribute.css('span ::text').extract()).strip()
        # post time
        post_time = response.css('#display-date > time:nth-child(1)::text').extract_first().strip()
        # timestamp
        post_retrieved_time = datetime.datetime.now().strftime('%Y-%m-%d %I:%M:%p')
        post_raw_html = response.body.decode("utf-8")
        search_url = response.meta['url']
        with open('./completed_urls.txt', 'a') as completed_urls:
            completed_urls.write(link+"\n")
       
        d= {
            "POST ID": str(post_id),
            "TITLE": title.split('hide this posting')[0],
            "DESCRIPTION": body_text,
            "ATTRIBUTES": attr_text,
            "NOTICES": notice_list,
            "SESSION ID": self.session_id,
            "LINK": link,
            "POST TIME": post_time,
            "POST RETRIEVED TIME": post_retrieved_time,
            "SEARCH URL": search_url,
            "RAW HTML": post_raw_html,
        }
        yield d
        print(len(d))
        self.counter+=1
        if (time() - self.start) > 780 :
            print("***************************")
            print("Going to sleep for 5 mins")
            sleep(300)
            print("***************************")
            self.start = time()

    #def close(self, reason):
        

        