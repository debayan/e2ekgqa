import sys,os,json,copy,torch
import requests
from elasticsearch import Elasticsearch
#from sentence_transformers import SentenceTransformer, util
#sentence_embeddings = model.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

es = Elasticsearch(host='ltcpu1',port=49158)

entembedcache = {}

def ft(labels): #fasttext
    r = requests.post("http://ltcpu1:49157/ftwv",json={'chunks': labels},headers={'Connection':'close'})
    descembedding = r.json()
    return descembedding

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
            print(entid,' entity embedding not found')
            return 200*[0.0]

dgold = json.loads(open(sys.argv[1]).read())

f = open(sys.argv[2],'w')

for idx,item in enumerate(dgold):
    print(idx)
    citem = copy.deepcopy(item)
    strarr = []
    embarr = []
    questionarr = item['question'].split()
    questionftembed = ft(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    embarr += [500*[-1.0]]
    for k,v in item['entlabelcands'].items():
        groundlabel = k
        groundembed = ft(groundlabel)
        labelembeds = ft([x['wikidataLabel'] for x in v])
        entembs = [kgembed(x['uri']) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,entembs)]
        strarr += [x['uri'] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,entembs)]
        strarr += ['[SEP]']
        embarr += [500*[-1.0]]
    for k,v in item['rellabelcands'].items():
        groundlabel = k
        groundembed = ft(groundlabel)
        #print(k, groundembed)
        labelembeds = ft([x[1] for x in v]) 
        relembs = [kgembed(x[0]) for x in v]
        #[print(x,y,z) for x,y,z in zip(v,labelembeds,relembs)]
        strarr += [x[0] for x in v]
        embarr += [x+y for x,y in zip(labelembeds,relembs)]
        strarr += ['[SEP]']
        embarr += [500*[-1.0]]
    print(strarr, len(strarr))
    #print(len(embarr))
    citem['vectorstring'] = strarr
    citem['vector'] = embarr
    f.write(json.dumps(citem)+'\n')
f.close()
