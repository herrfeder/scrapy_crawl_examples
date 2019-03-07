import time
import hashlib
from elasticsearch import Elasticsearch
import spacy

nlp_ne = spacy.load('xx_ent_wiki_sm')
nlp_to = spacy.load('de_core_news_sm')

def convert_date(date,country):
    
    if "unknown" in date:
            return "2000-01-01"
    result = date
    try:
        if country == "ge":
            date_temp = date.split(".")
            # compute year
            year = date_temp[2]
            if year.startswith("20"):
                pass
            else:
                year = "20"+year
            # compute day
            try:
                day = date_temp[0].split(": ")[1]
            except:
                day = date_temp[0]
            # compute month
            month = date_temp[1]

            result = "{}-{}-{}".format(year,month,day)
            return result
    except:
        Tracer()()
        return result



def get_text_by_label(labels,texts,key):
    dups = duplicates(labels,key)
    text_temp = [texts[x] for x in dups]

    return text_temp


def duplicates(lst, item):
    return [i for i, x in enumerate(lst) if x == item]


def get_text_hash(text):
    
    hash_object = hashlib.sha1(bytes(text,'utf-8'))
    hex_dig = str(hash_object.hexdigest())
    return hex_dig


def nlp_and_parse_el(final_site_text):

    ent_dict = {}
    # apply named entity training set
    doc_ne = nlp_ne(final_site_text)

    doc_tok = nlp_to(final_site_text)

    #for token in doc_tok:
    #    print(token.text, token.pos_, token.dep_)

    # select for labels
    labels = [ x.label_ for x in doc_ne.ents]
    if "ORG" in labels or "LOC" in labels or "PER" in labels:
        ent_texts = [x.text for x in doc_ne.ents]

        # remove duplicate entities
        for ent_text in ent_texts:
            dups = duplicates(ent_texts,ent_text)
            if len(dups) > 1:
                for dup in dups[1::]:
                    labels[dup]="_x_"
                    ent_texts[dup]="_x_"

        labels = [x for x in labels if not x.startswith("_x_")]
        ent_texts = [x for x in ent_texts if not x.startswith("_x_")]

        ent_dict = {}
        ent_dict['ORG'] = get_text_by_label(labels,ent_texts,"ORG")
        ent_dict['MISC'] = get_text_by_label(labels,ent_texts,"MISC")
        ent_dict['LOC'] = get_text_by_label(labels,ent_texts,"LOC")
        ent_dict['PER'] = get_text_by_label(labels,ent_texts,"PER")


    return ent_dict

def push_el(ent_dict,index):

    es_client = Elasticsearch(['http://127.0.0.1:9200'])

    # check for already existing text
    search_object = {"query": {"match": {"hash":ent_dict['hex_dig']}}} 
    res = es_client.search("bwde", doc_type="docs", body=search_object)
    if res['hits']['total'] != 0:
        return -2
    # payload for elasticsearch 
    doc = {
        'crawled': time.strftime("%Y-%m-%d"),
        'date': ent_dict['date'],
        'title': ent_dict['title_name'],
        'ORG': ent_dict['ORG'] ,
        'LOC': ent_dict['LOC'],
        'MISC': ent_dict['MISC'],
        'PER': ent_dict['PER'],
        'fulltext': ent_dict['final_site_text'],
        'url': ent_dict['url'],
        'hash':ent_dict['hex_dig']}

    # push doc to elasticsearch index
    res = es_client.index(index="bwde", doc_type="docs", body=doc)
    #print(res)
    time.sleep(0.5)
    return 1

