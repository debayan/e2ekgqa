from sentence_transformers import SentenceTransformer, util
import os
import csv
import pickle
import time
from elasticsearch import Elasticsearch
import torch
import json
from annoy import AnnoyIndex

es = Elasticsearch(host='ltcpu1',port=49158)


model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)

max_corpus_size = 100000

n_trees = 256           #Number of trees used for Annoy. More trees => better recall, worse run-time
embedding_size = 768    #Size of embeddings
top_k_hits = 10         #Output k hits

def getlabel(ent):
    res = es.search(index="wikidataentitylabelindex02", body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        return res['hits']['hits'][0]['_source']['wikidataLabel']
    except Exception as err:
        print(err)
        return ''


annoy_index = AnnoyIndex(embedding_size, 'angular')
annoy_index.load('annoy-p31.ann')
ents = json.loads(open('annoyentids.json').read())


while True:
    inp_question = input("Please enter a question: ")

    start_time = time.time()
    question_embedding = model.encode(inp_question)

    corpus_ids, scores = annoy_index.get_nns_by_vector(question_embedding, top_k_hits, include_distances=True)
    hits = []
    for id, score in zip(corpus_ids, scores):
        hits.append({'corpus_id': id, 'score': 1-((score**2) / 2)})

    end_time = time.time()

    print("Input question:", inp_question)
    print("Results (after {:.3f} seconds):".format(end_time-start_time))
    for hit in hits[0:top_k_hits]:
        print("\t{:.3f}\t{}\t{}".format(hit['score'], ents[hit['corpus_id']], getlabel(ents[hit['corpus_id']])))

    print("\n\n========\n")
