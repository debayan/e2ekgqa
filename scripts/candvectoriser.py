import sys,os,json,copy,torch
import requests
from elasticsearch import Elasticsearch

from sentence_transformers import SentenceTransformer, util

model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)



es = Elasticsearch(host='ltcpu1',port=49158)

entembedcache = {}

def ft_(labels): #fasttext
    r = requests.post("http://ltcpu1:49157/ftwv",json={'chunks': labels},headers={'Connection':'close'})
    descembedding = r.json()
    return descembedding

def tremb(labels): #transformers
    sentence_embeddings = model.encode(labels, convert_to_tensor=True)
    return sentence_embeddings.tolist()


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

dgold = json.loads(open(sys.argv[1]).read())

f = open(sys.argv[2],'w')

for idx,item in enumerate(dgold):
    print(idx,item)
    citem = copy.deepcopy(item)
    strarr = []
    embarr = []
    questionarr = item['question'].split()
    questionftembed = tremb(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
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
        labelembeds = tremb([x[1] if x[1] else 'null' for x in v]) 
        relembs = [kgembed(x[0]) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x[0] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [968*[-1.0]]
    #print(strarr, len(strarr))
    #print(len(embarr))
    citem['vectorstring'] = strarr
    citem['vector'] = embarr
    f.write(json.dumps(citem)+'\n')
f.close()
