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



model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)

max_corpus_size = 100000

n_trees = 256           #Number of trees used for Annoy. More trees => better recall, worse run-time
embedding_size = 768    #Size of embeddings
top_k_hits = 10         #Output k hits

annoy_index_path = 'annoy-embeddings-{}-size-{}-annoy_index-trees-{}.ann'.format(model_name.replace('/', '_'), max_corpus_size,n_trees)
embedding_cache_path = 'annoy-embeddings-{}-size-{}.pkl'.format(model_name.replace('/', '_'), max_corpus_size)


# Define config
host = "ltcpu1"
port = 49158
timeout = 1000
index = "wikidataentitylabelindex02"
size = 10000
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
def process_hits(entids,annoy,hits):
	corpus_sentences = [x['_source']['wikidataLabel'] for x in hits]
	enturis = [x['_source']['uri'] for x in hits]
	corpus_sentences = list(corpus_sentences)
    #print("Encode the corpus. This might take a while")
	corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_numpy=True)
    #print("Store file on disc")
    #with open(embedding_cache_path, "wb") as fOut:
    #    pickle.dump({'sentences': corpus_sentences, 'embeddings': corpus_embeddings}, fOut)
	#print("Create Annoy index with {} trees. This can take some time.".format(n_trees))

	for enturi,emb in zip(enturis,corpus_embeddings):
		annoy_index.add_item(len(entids), emb)
		entids.append(enturi)
	print(len(entids))

    #for item in hits:
    #    print(json.dumps(item, indent=2))


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
entids = []

while scroll_size > 0:
    "Scrolling..."
    
    # Before scroll, process current batch of hits
    process_hits(entids,annoy_index,data['hits']['hits'])
    
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])
    print(len(entids))

f = open('annoyentids.json','w')
f.write(json.dumps(entids))
f.close()
annoy_index.build(n_trees)
annoy_index.save(annoy_index_path)
