import sys,os,json,copy,torch,re
import requests
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util
from random import randrange

model_name = 'quora-distilbert-multilingual'
model = SentenceTransformer(model_name)

trembdict = {}

def tremb(labels): #transformers
    l = ' '.join(labels)
    if l in trembdict:
        return trembdict[l]
    else:
        sentence_embeddings = model.encode(labels, convert_to_tensor=True) 
        trembdict[l] = sentence_embeddings.tolist()
        return trembdict[l]


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
    if len(embarr) != len(strarr):
        print("up:",strarr)
        print("ERROR")
        sys.exit(1)
    #print(len(embarr))
    citem['goldentrelvectorstring'] = strarr
    citem['goldentrelvector'] = embarr
    f.write(json.dumps(citem)+'\n')
    #random deletion of an ent or rel
    strarr = item['question'].replace('?','').replace('-',' - ').split()
    print("strarr:",strarr)
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    embarr += [968*[-1.0]]
    maskcitem = copy.deepcopy(item)
    if not 'select' in wikisparql.lower():
        continue
    print(ents,rels)
    mask = False
    for iidx,cand in enumerate(ents+rels):
        cand = cand.replace('}','').replace('.','')
        if not cand:
            continue
        if cand[0] == 'P':
            r = randrange(len(ents+rels))
            print(r,iidx,mask)
            if r == iidx and not mask:
               masksparql = []
               for word in wikisparql.split():
                   if cand in word: 
                       masksparql.append('?maskvar1')
                   else:
                      masksparql.append(word)
               newsparql = ' '.join(masksparql)
               newsparql = newsparql.lower().replace('select distinct', 'select distinct ?maskvar1' ).replace('select ?','select ?maskvar1 ?')
               maskcitem['sparql_wikidata'] = newsparql
               mask = True
               print(wikisparql, newsparql) 
            else:
                if cand in props:
                    rellabel = props[cand]
                    relftembed = tremb([rellabel])[0]
                else:
                    relftembed = 768*[0.0]
                relkgembed = kgembed(cand)
                embarr += [relftembed+relkgembed]
                strarr += [cand]
        if cand[0] == 'Q':
            r = randrange(len(ents+rels))
            print(r,iidx,mask)
            if r == iidx and not mask:
               masksparql = []
               for word in wikisparql.split():
                   if cand in word:
                       masksparql.append('?maskvar1')
                   else:
                      masksparql.append(word)
               newsparql = ' '.join(masksparql)
               newsparql = newsparql.lower().replace('select distinct', 'select distinct ?maskvar1' ).replace('select ?','select ?maskvar1 ?')
               maskcitem['sparql_wikidata'] = newsparql
               mask = True
               print(wikisparql,newsparql) 
            else:
                entlabel = getlabel(cand)
                if entlabel:
                    entftembed = tremb([entlabel])[0]
                else:
                    entftembed = 768*[0.0]
                entkgembed = kgembed(cand)
                embarr += [entftembed+entkgembed]
                strarr += [cand]
    if len(embarr) != len(strarr):
        print("down:",strarr)
        print("ERROR")
        sys.exit(1)
    maskcitem['goldentrelvectorstring'] = strarr
    maskcitem['goldentrelvector'] = embarr
    f.write(json.dumps(maskcitem)+'\n')
f.close()
