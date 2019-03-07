#!/usr/bin/python
# -*- coding: iso-8859-15 -*-

from bs4 import BeautifulSoup as bs
import re
import time
import requests
from IPython.core.debugger import Tracer
from crawl_bw.spiders import utility as u


def general_parser(page, url, digest_array):

    # index site with beautiful soup
    
    ### this only important when running without scrapy
    #page = requests.get(page).content
    soup = bs(page, 'lxml')

    # get title
    title_name = soup.title.string

    # footer with date and author
    date = "unknown"
    author = "unkown"
    site_text = ""

    if "www.bundeswehr.de" in url:
        date,author,site_text = urlparser_bwde(soup)

    # remove source code texts
    if "()" in site_text:
        site_text = ""

    hex_dig = u.get_text_hash(site_text)

    if hex_dig in digest_array:
        print("We scraped that already")
    elif hex_dig not in digest_array:
        digest_array.append(hex_dig)
        
        # process text
        site_texts = site_text.split('__')
        site_texts = [x for x in site_texts if not x == "\n"]
        final_site_text = ".".join(site_texts)

        result_dict = u.nlp_and_parse_el(final_site_text)
       
        if result_dict:

            result_dict["date"] = date
            result_dict["title_name"] = title_name
            result_dict["final_site_text"] = final_site_text
            result_dict["url"] = url
            result_dict["hex_dig"] = hex_dig

            res = u.push_el(result_dict,"bwde")
            if res == -2:
                print("We got that already in Database")


def urlparser_bwde(soup):

    date="unknown"
    author="unknown"

    # scrape author and date from footer
    footerstatus = str(soup.findAll('p',id='footerstatus')[0])
    if not "Stand" in footerstatus:
        pass
    else:
        if "|" in footerstatus:
            date = footerstatus.split("Stand vom: ")[1].split(" ")[0]
            author = footerstatus.split("Autor:")[1].split("\n")[0]
        else:
            date = footerstatus.split("Stand vom")[1].split("\n")[0] 
            author = "unknown"


    # get sitetext, it's only organized by p tags, therefore it's difficult
    # to exclude links and other stuff
    site_text = soup.findAll('div',id='content')[0].get_text('__')
    
    return u.convert_date(date,"ge"),author,site_text


'''    
urls = []

root_url = 'https://www.bundeswehr.de'
sitemap_feed = 'https://www.bundeswehr.de/portal/a/bwde/start/service-sitemap/'
page = requests.get(sitemap_feed)
sitemap_index = bs(page.content, 'html.parser')

links = sitemap_index.findAll('li')

for link in links:
    ex_link = "https"
    try:
        ex_link = str(link.findAll('a')[0]['href'])
    except:
        pass
    if ex_link.startswith("/"):
        urls.append(root_url+ex_link)
    else:
        pass


digest_array = []

for x in urls:
    print("Scraping URL: {}".format(x))
    general_parser(x, root_url,digest_array)
'''
