import os
import json
import sys
import requests
import torch.multiprocessing
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util
from annoy import AnnoyIndex
from Neighbours2021 import Neighbours

model_name = 'quora-distilbert-multilingual'
sentencemodel = SentenceTransformer(model_name)
top_k_hits = 30         #Output k hits
annoy_index = AnnoyIndex(768, 'angular')
annoy_index.load('annoy-p31.ann')
annoyents = json.loads(open('annoyentids.json').read())

n = Neighbours()

reldict = json.loads(open('en1.json').read())
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
    labels = [x if x else 'null' for x in labels]
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
        #print(entid,' entity embedding not found')
        return 200*[0.0]

def sparqlendpoint(query):
    url = 'http://ltcpu3:8890/sparql'
    r = requests.get(url, params = {'format': 'json', 'query': query})
    try:
        data = r.json()
        return data
    except Exception as err:
        print(err)
        return {"error":repr(err), "errorcode":r.status_code}


rellabelcache = {}
def rellabel(rel):
    if rel in rellabelcache:
        return rellabelcache[rel]
    if rel in reldict:
        return reldict[rel]
    else:
        sparql = '''PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> 
                    PREFIX wd: <http://www.wikidata.org/entity/> 
                    SELECT  *
                       WHERE {
                            wd:%s rdfs:label ?label .
                            FILTER (langMatches( lang(?label), "EN" ) )
                       } 
                    LIMIT 1'''%(rel)
        result = sparqlendpoint(sparql)
        if result:
            if result['results']['bindings']:
                label = result['results']['bindings'][0]['label']['value']
                print(rel,label)
                if not label:
                    return 'null'
                rellabelcache[rel] = label
                return label
        else:
            return 'null'

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

def getneighbours(entid):
    return n.fetch_neighbours_relations(entid) 

def vectorise(question,labels):
    entss = ''
    relss = ''
    try:
        entss,relss = labels.split('//')
    except Exception as err:
        print(err, labels)
        entss = labels
    try:
        ents = [x.strip() for x in entss.split('::')]
        rels = [x.strip() for x in relss.split(';;')]
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
    questionfullftembed = tremb([question])[0]
    questionftembed = tremb(questionarr)
    searchrank = -1 #makes no sense for non ents and rels, hence -1
    strarr += ['QEMB']
    embarr += [[searchrank]+questionfullftembed+200*[0.0]]
    strarr += ['[SEP]']
    embarr += [969*[-1.0]]
    strarr += questionarr
    embarr += [[searchrank]+x+200*[0.0] for x in questionftembed]
    strarr += ['[SEP]']
    embarr += [969*[-1.0]]
#    pnelents = pnelentcands(question)
#    print("pnel ents:",pnelents['entities'])
#    if len(pnelents['entities']) > 0:
#        for k,v in pnelents['entities'].items():
#            seen = []
#            for entcand in v:
#                if entcand[0] not in seen:
#                   labelembed = tremb([entcand[3]])[0]
#                   entemb = kgembed(entcand[0])
#                   strarr += [entcand[0]]
#                   embarr += [labelembed + entemb]
#                   seen.append(entcand[0])
#    strarr += ['[SEP]']
#    embarr += [968*[-1.0]]
    relneighbours = []
    for k,v in entlabelcands.items():
        if len(v) == 0:
            continue
        labelembeds = tremb([x['wikidataLabel'] for x in v])
        entembs = [kgembed(x['uri']) for x in v]
        for x in v:
            relneighbours += getneighbours(x['uri'])
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x['uri'] for x in v]
        rank = 0
        for x,y in zip(labelembeds,entembs):
            embarr += [[rank]+x+y]
            rank += 1
        strarr += ['[SEP]']
        embarr += [969*[-1.0]]
    for k,v in annentlabelcands.items():
        if len(v) == 0:
            continue
        labelembeds = tremb([getlabel(x) for x in v])
        entembs = [kgembed(x) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x for x in v]
        rank = 0
        for x,y in zip(labelembeds,entembs):
            embarr += [[rank]+x+y]
            rank += 1
        strarr += ['[SEP]']
        embarr += [969*[-1.0]]
    for k,v in rellabelcands.items():
        if len(v) == 0:
            continue
        #print(k, groundembed)
        labelembeds = tremb([rellabel(x[0]) for x in v])
        relembs = [kgembed(x[0]) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x[0] for x in v]
        rank = 0
        for x,y in zip(labelembeds,entembs):
            embarr += [[rank]+x+y]
            rank += 1
        strarr += ['[SEP]']
        embarr += [969*[-1.0]]
    # one and on half hop rel neighbours for entities
    neighbours = list(set(relneighbours))
    print("lenneighbours",len(neighbours))
    if len(neighbours) > 0:
        labelembeds = tremb([rellabel(x) for x in neighbours])
        relembs = [kgembed(x) for x in neighbours]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x for x in neighbours]
        embarr += [[-1]+x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [969*[-1.0]]
    return strarr,embarr
