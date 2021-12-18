# coding:utf-8

from elasticsearch import Elasticsearch
import json
from sentence_transformers import SentenceTransformer, util
import os
import csv
import pickle
import time
import torch
from annoy import AnnoyIndex
import gensim
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

# Load Google's pre-trained Word2Vec model.
w2vmodel = gensim.models.KeyedVectors.load_word2vec_format('./GoogleNews-vectors-negative300.bin', binary=True)  

max_corpus_size = 100000

n_trees = 256           #Number of trees used for Annoy. More trees => better recall, worse run-time
embedding_size = 300    #Size of embeddings
top_k_hits = 10         #Output k hits

annoy_index_path = 'annoy-w2v.ann'


# Define config
host = "ltcpu1"
port = 49158
timeout = 1000
index = "wikidataentitylabelindex02"
size = 1000
body = {}

# Init Elasticsearch instance
es = Elasticsearch(
    [
        {
            'host': host,
            'port': port
        }
    ],
    timeout=timeout
)

# Process hits here
def process_hits(entsarr,annoy,hits):
	corpus_sentences = [x['_source']['wikidataLabel'] for x in hits]
	enturis = [x['_source']['uri'] for x in hits]
	corpus_sentences = list(corpus_sentences)
	for enturi,sentence in zip(enturis,corpus_sentences):
		text_tokens = word_tokenize(sentence)
		tokens_without_sw = [word for word in text_tokens if not word in stopwords.words()]
		try:
			mean = np.mean(np.array([w2vmodel[x] for x in tokens_without_sw]), axis=0)
			annoy_index.add_item(len(entsarr),mean)
			entsarr.append(enturi)
		except Exception as err:
			pass

# Check index exists
if not es.indices.exists(index=index):
    print("Index " + index + " not exists")
    exit()

# Init scroll by search
data = es.search(
    index=index,
    scroll='2m',
    size=size,
    body=body
)

# Get the scroll ID
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])
annoy_index = AnnoyIndex(embedding_size, 'angular')
counter = 0
entsarr = []

while scroll_size > 0:
    "Scrolling..."
    
    # Before scroll, process current batch of hits
    process_hits(entsarr,annoy_index,data['hits']['hits'])
    
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Update the scroll ID
    sid = data['_scroll_id']
    print(counter*size)
    counter += 1
    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])


f = open('annoyentids.json','w')
f.write(json.dumps(entsdict))
f.close()
annoy_index.build(n_trees)
annoy_index.save(annoy_index_path)
