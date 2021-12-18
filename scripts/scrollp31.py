# coding:utf-8

from elasticsearch import Elasticsearch
import json
import os
import csv
import pickle
import time
import torch
from annoy import AnnoyIndex
from sentence_transformers import SentenceTransformer, util

es = Elasticsearch(host='ltcpu1',port=49158)
model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)
max_corpus_size = 100000

n_trees = 128           #Number of trees used for Annoy. More trees => better recall, worse run-time
embedding_size = 768    #Size of embeddings
top_k_hits = 10         #Output k hits

annoy_index_path = 'annoy-p31.ann'


def getlabel(ent):
	res = es.search(index="wikidataentitylabelindex02", body={"query":{"term":{"uri":{"value":ent}}}})
	try:
		return res['hits']['hits'][0]['_source']['wikidataLabel']
	except Exception as err:
		print(err)
		return ''

# Process hits here
def process_hits(emptyentarr,annoy_index,filledentarr):
	sentences = []
	for idx,ent in enumerate(filledentarr):
		print(idx)
		label = getlabel(ent)
		if label:
			sentences.append(label)
			emptyentarr.append(ent)
	print("fetched all labels")
	corpus_embeddings = model.encode(sentences, show_progress_bar=True, convert_to_numpy=True)
	print("computed embeddings")

	for idx,emb in enumerate(corpus_embeddings):
		annoy_index.add_item(idx, emb)
	print("added to treees")

    #for item in hits:
    #    print(json.dumps(item, indent=2))


annoy_index = AnnoyIndex(embedding_size, 'angular')
emptyentarr = []
filledentarr = json.loads(open('subclassentslist.json').read())

process_hits(emptyentarr,annoy_index,filledentarr)
    
f = open('annoyentids.json','w')
f.write(json.dumps(emptyentarr))
f.close()
annoy_index.build(n_trees)
annoy_index.save(annoy_index_path)
