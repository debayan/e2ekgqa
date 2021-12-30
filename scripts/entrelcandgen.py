import sys,os,json,copy,torch
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util
import csv
import torch
import json
from annoy import AnnoyIndex
from Neighbours2021 import Neighbours

n = Neighbours()
es = Elasticsearch(host='ltcpu1',port=49158)


model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)

top_k_hits = 30         #Output k hits


annoy_index = AnnoyIndex(768, 'angular')
annoy_index.load('annoy-p31.ann')
annoyents = json.loads(open('annoyentids.json').read())

propdict = json.loads(open('en1.json').read())
goldrellabels = []

for k,v in propdict.items():
    goldrellabels.append([k,v])
sentence_embeddings = model.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

dgold = json.loads(open(sys.argv[1]).read())
es = Elasticsearch(host='ltcpu1',port=49158)


def getneighbours(entid):
    return n.fetch_neighbours_relations(entid)

relcanddict = {}
def relcands(rellabel):
    if rellabel in relcanddict:
        return relcanddict[rellabel]   
    results = []
    query = rellabel.strip()
    if not query:
        return []
    query_embedding = model.encode(query, convert_to_tensor=True)

    # We use cosine-similarity and torch.topk to find the highest 5 scores
    cos_scores = util.pytorch_cos_sim(query_embedding, sentence_embeddings)[0]
    top_results = torch.topk(cos_scores, k=30)

    #print("\n\n======================\n\n")
    #print("Query:", query)
    #print("\nTop 5 most similar sentences in corpus:")

    for score, idx in zip(top_results[0], top_results[1]):
        #print(goldrellabels[idx], "(Score: {:.4f})".format(score))
        results.append(goldrellabels[idx])
    relcanddict[rellabel] = results
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

def annmatch(entlabel):
    question_embedding = model.encode(entlabel)
    corpus_ids, scores = annoy_index.get_nns_by_vector(question_embedding, top_k_hits, include_distances=True)
    return [annoyents[id] for id in corpus_ids]
	

goldarr = []
for idx,item in enumerate(dgold): 
    citem = copy.deepcopy(item)
    uid = item['uid']
    #predlabels = item['predlabel']
    goldlabels = item['labels']
    entss = ''
    relss = ''
    try:
        entss,relss = goldlabels.split('//')
    except Exception as err:
        print(err, goldlabels)
        entss = goldlabels
    try:
        ents = [x.strip() for x in entss.split('::')]
        rels = [x.strip() for x in relss.split(';;')]
    except Exception as err:
        print(err)
        continue
    print(idx)
    print("goldlabels:", goldlabels)
    #print("predlabels:",predlabels)
    print("ents      :",ents)
    print("rels      :",rels)
    citem['entlabelcands'] = {}
    citem['annentlabelcands'] = {}
    citem['rellabelcands'] = {}
    citem['neighbours'] = {}
    for ent in ents:
        results = entcands(ent)
        citem['entlabelcands'][ent] = results
        for entid in results:
            citem['neighbours'][entid['uri']] = getneighbours(entid['uri'])
#        print("entlabelcands:",results)
    for ent in ents:
        results = annmatch(ent)
        citem['annentlabelcands'][ent] = results
#        print("annentlabelcans:",results)
    for rel in rels:
        results = relcands(rel)
        citem['rellabelcands'][rel] = results
#        print("rellabelcands:",results)
    goldarr.append(citem)
    if idx%1000 == 0:
        f = open(sys.argv[2],'w')
        f.write(json.dumps(goldarr,indent=4))
        f.close()

f = open(sys.argv[2],'w')
f.write(json.dumps(goldarr,indent=4))
f.close()
