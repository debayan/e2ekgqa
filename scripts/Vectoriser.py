import logging,os
import json
import sys
import requests
import torch.multiprocessing
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util
from annoy import AnnoyIndex

model_name = 'quora-distilbert-multilingual'
sentencemodel = SentenceTransformer(model_name)
top_k_hits = 30         #Output k hits
annoy_index = AnnoyIndex(768, 'angular')
annoy_index.load('annoy-p31.ann')
annoyents = json.loads(open('annoyentids.json').read())


reldict = json.loads(open('en.json').read())
goldrellabels = []

for k,v in reldict.items():
    goldrellabels.append([k,v])

sentence_embeddings = sentencemodel.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

es = Elasticsearch(host='ltcpu1',port=49158)

def annmatch(entlabel):
    question_embedding = sentencemodel.encode(entlabel)
    corpus_ids, scores = annoy_index.get_nns_by_vector(question_embedding, top_k_hits, include_distances=True)
    return [annoyents[id] for id in corpus_ids]

def tremb(labels): #transformers
    sentence_embeddings = sentencemodel.encode(labels, convert_to_tensor=True)
    return sentence_embeddings.tolist()

def getlabel(ent):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        for res in results['hits']['hits']:
            return res['_source']['wikidataLabel']
    except Exception as err:
        print(results)
        print(ent,err)
        return 'null'

def kgembed(entid): #fasttext
    enturl = '<http://www.wikidata.org/entity/'+entid+'>'
    res = es.search(index="wikidataembedsindex01", body={"query":{"term":{"key":{"value":enturl}}}})
    try:
        embedding = [float(x) for x in res['hits']['hits'][0]['_source']['embedding']]
        return embedding
    except Exception as e:
        print(entid,' entity embedding not found')
        return 200*[0.0]

def relcands(rellabel):
    results = []
    query = rellabel.strip()
    if not query:
        return []
    query_embedding = sentencemodel.encode(query, convert_to_tensor=True)

    # We use cosine-similarity and torch.topk to find the highest 5 scores
    cos_scores = util.pytorch_cos_sim(query_embedding, sentence_embeddings)[0]
    top_results = torch.topk(cos_scores, k=30)

    #print("\n\n======================\n\n")
    #print("Query:", query)
    #print("\nTop 5 most similar sentences in corpus:")

    for score, idx in zip(top_results[0], top_results[1]):
        #print(goldrellabels[idx], "(Score: {:.4f})".format(score))
        results.append(goldrellabels[idx])
    return results

def entcands(entlabel):
    esresults = es.search(index='wikidataentitylabelindex02',body={"query":{"match":{"wikidataLabel":entlabel}}},size=30)
    results = []
    try:
        for res in esresults['hits']['hits']:
            #print(entlabel, res['_source'])
            results.append(res['_source'])
        return results
    except Exception as err:
        print(entlabel,err)
        return results

def pnelentcands(question):
    r = requests.post("http://ltdemos.informatik.uni-hamburg.de/pnel/processQuery",json={"nlquery":question},headers={'Connection':'close'})
    ents = r.json()
    return ents


def vectorise(question,labels):
    entss = ''
    relss = ''
    try:
        entss,relss = labels.split('//')
    except Exception as err:
        print(err, labels)
        entss = labels
    try:
        ents = entss.split('::')
        rels = relss.split(';;')
    except Exception as err:
        print("error:",err)
        return [],[]
    print("labels:",labels)
    print("ents      :",ents)
    print("rels      :",rels)
    entlabelcands = {}
    annentlabelcands = {}
    rellabelcands = {}
    for ent in ents:
        entlabelcands[ent] =  entcands(ent)
        annentlabelcands[ent] = annmatch(ent)
    for rel in rels:
        rellabelcands[rel] =  relcands(rel)
    strarr = []
    embarr = []
    questionarr = question.split()
    questionftembed = tremb(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    embarr += [968*[-1.0]]
#    pnelents = pnelentcands(question)
#    print("pnel ents:",pnelents['entities'])
#    for k,v in pnelents['entities'].items():
#        seen = []
#        for entcand in v:
#            if entcand[0] not in seen:
#                labelembed = tremb([entcand[3]])[0]
#                entemb = kgembed(entcand[0])
#                strarr += [entcand[0]]
#                embarr += [labelembed + entemb]
#                seen.append(entcand[0])
#    strarr += ['[SEP]']
#    embarr += [968*[-1.0]]
    for k,v in entlabelcands.items():
        if len(v) == 0:
            continue
        labelembeds = tremb([x['wikidataLabel'] for x in v])
        entembs = [kgembed(x['uri']) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x['uri'] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,entembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    for k,v in annentlabelcands.items():
        if len(v) == 0:
            continue
        labelembeds = tremb([getlabel(x) for x in v])
        entembs = [kgembed(x) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x for x in v]
        embarr += [x+y for x,y in zip(labelembeds,entembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    for k,v in rellabelcands.items():
        if len(v) == 0:
            continue
        #print(k, groundembed)
        labelembeds = tremb([x[1] for x in v])
        relembs = [kgembed(x[0]) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x[0] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    return strarr,embarr


