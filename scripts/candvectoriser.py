import sys,os,json,copy,torch
import requests
from elasticsearch import Elasticsearch
from fuzzywuzzy import fuzz

from sentence_transformers import SentenceTransformer, util

model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)

propdict = json.loads(open('en1.json').read())

es = Elasticsearch(host='ltcpu1',port=49158)

entembedcache = {}

trembdict = {}

def tremb(labels): #transformers
    labels = [x if x else 'null' for x in labels]
    l = ' '.join(labels)
    if l in trembdict:
        return trembdict[l]
    else:
        sentence_embeddings = model.encode(labels, convert_to_tensor=True)
        trembdict[l] = sentence_embeddings.tolist()
        return trembdict[l]


def kgembed(entid): #fasttext
    if entid in entembedcache:
        return entembedcache[entid]
    else:
        enturl = '<http://www.wikidata.org/entity/'+entid+'>'
        res = es.search(index="wikidataembedsindex01", body={"query":{"term":{"key":{"value":enturl}}}})
        try:
            embedding = [float(x) for x in res['hits']['hits'][0]['_source']['embedding']]
            entembedcache[entid] = embedding
            return embedding
        except Exception as e:
            #print(entid,' entity embedding not found')
            return 200*[0.0]

def getlabel(ent):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        for res in results['hits']['hits']:
            return res['_source']['wikidataLabel']
    except Exception as err:
        #print(results)
        #print(ent,err)
        return 'null'

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
    if rel in propdict:
        return propdict[rel]
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

dgold = json.loads(open(sys.argv[1]).read())

f = open(sys.argv[2],'w')

for idx,item in enumerate(dgold):
    print(idx,item)
    citem = copy.deepcopy(item)
    strarr = []
    embarr = []
    if not item['question']:
        continue
    questionarr = item['question'].split()
    questionftembed = tremb(questionarr)
    questionfullftembed = tremb([item['question']])[0]
    strarr += ['QEMB']
    embarr += [questionfullftembed+200*[0.0]]
    strarr += ['[SEP]']
    embarr += [968*[-1.0]]
    strarr += questionarr
    embarr += [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    embarr += [968*[-1.0]]
    for k,v in item['entlabelcands'].items():
        if len(v) == 0:
            continue
        labelembeds = tremb([x['wikidataLabel'] for x in v])
        entembs = [kgembed(x['uri']) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x['uri'] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,entembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    for k,v in item['annentlabelcands'].items():
        if len(v) == 0:
            continue
        labelembeds = tremb([getlabel(x) for x in v])
        entembs = [kgembed(x) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x for x in v]
        embarr += [x+y for x,y in zip(labelembeds,entembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    for k,v in item['rellabelcands'].items():
        if len(v) == 0:
            continue
        #print(k, groundembed)
        labelembeds = tremb([rellabel(x[0]) for x in v]) 
        relembs = [kgembed(x[0]) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x[0] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    neighbours = []
    for k,v in item['neighbours'].items():
        if len(v) == 0:
            continue
        for rel in v:
            neighbours.append(rel)
    neighbours = list(set(neighbours))
        #print(k, groundembed)
    if len(neighbours) > 0:
        labelembeds = tremb([rellabel(x) for x in neighbours])
        relembs = [kgembed(x) for x in neighbours]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x for x in neighbours]
        embarr += [x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    #print(strarr, len(strarr))
    #print(len(embarr))
    citem['vectorstring'] = strarr
    citem['vector'] = embarr
    f.write(json.dumps(citem)+'\n')
    sys.exit(1)
f.close()
