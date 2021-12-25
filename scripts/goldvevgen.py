import sys,os,json,copy,torch,re
import requests
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util

model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)



def tremb(labels): #transformers
    sentence_embeddings = model.encode(labels, convert_to_tensor=True)
    return sentence_embeddings.tolist()


es = Elasticsearch(host='ltcpu1',port=49158)
props = json.loads(open('en.json').read())
entembedcache = {}


def getlabel(ent):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        for res in results['hits']['hits']:
            return res['_source']['wikidataLabel']
    except Exception as err:
        #print(results)
        #print(ent,err)
        return 'null'

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
    src = item['question']
    src = src.replace('?','').replace('-',' - ')
    questionarr = src.split()
    if not questionarr:
        continue
    wikisparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
    ents = citem['ents']
    rels = citem['rels']
    print(wikisparql,ents,rels)
    questionftembed = tremb(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    embarr += [968*[-1.0]]
    for cand in ents+rels:
        cand = cand.replace('}','').replace('.','')
        if not cand:
            continue
        if cand[0] == 'P':
            if cand in props:
                rellabel = props[cand]
                relftembed = tremb([rellabel])[0]
            else:
                relftembed = 768*[0.0]
            relkgembed = kgembed(cand)
            embarr += [relftembed+relkgembed]
            strarr += [cand]
            continue
        if cand[0] == 'Q':
            entlabel = getlabel(cand)
            if entlabel:
                entftembed = tremb([entlabel])[0]
            else:
                entftembed = 768*[0.0]
            entkgembed = kgembed(cand)
            embarr += [entftembed+entkgembed]
            strarr += [cand]
            continue
    #print(len(embarr))
    citem['goldentrelvectorstring'] = strarr
    citem['goldentrelvector'] = embarr
    f.write(json.dumps(citem)+'\n')

f.close()
