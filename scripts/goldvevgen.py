import sys,os,json,copy,torch,re
import requests
from elasticsearch import Elasticsearch
#from sentence_transformers import SentenceTransformer, util
#sentence_embeddings = model.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

es = Elasticsearch(host='ltcpu1',port=49158)
props = json.loads(open('en.json').read())
entembedcache = {}

def getentlabel(entid):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":entid}}}})
    try:
        for res in results['hits']['hits']:
            return res['_source']['wikidataLabel']
    except Exception as err:
        print(results)
        print(ent,err)
        return ''


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
    src = item['question']
    src = src.replace('?','').replace('-',' - ')
    questionarr = src.split()
    wikisparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
    ents = re.findall( r'wd:(.*?) ',wikisparql)
    rels = re.findall( r'wdt:(.*?) ',wikisparql)
    rels += re.findall( r'p:(.*?) ',wikisparql)
    rels += re.findall( r'ps:(.*?) ',wikisparql)
    rels += re.findall( r'pq:(.*?) ',wikisparql)
    print(wikisparql,ents,rels)
    questionftembed = ft(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    citem['goldentrel'] = ents + rels
    embarr += [500*[-1.0]]
    for cand in citem['goldentrel']:
        cand = cand.replace('}','').replace('.','')
        if not cand:
            continue
        if cand[0] == 'P':
            if cand in props:
                rellabel = props[cand]
                relftembed = ft([rellabel])[0]
            else:
                relftembed = 300*[0.0]
            relkgembed = kgembed(cand)
            embarr += [relftembed+relkgembed]
            strarr += [cand]
            continue
        if cand[0] == 'Q':
            entlabel = getentlabel(cand)
            if entlabel:
                entftembed = ft([entlabel])[0]
            else:
                entftembed = 300*[0.0]
            entkgembed = kgembed(cand)
            embarr += [entftembed+entkgembed]
            strarr += [cand]
            continue
    #print(len(embarr))
    citem['goldentrelvectorstring'] = strarr
    citem['goldentrelvector'] = embarr
    f.write(json.dumps(citem)+'\n')
for idx,item in enumerate(dgold):
    print(idx)
    citem = copy.deepcopy(item)
    strarr = []
    embarr = []
    if not item['paraphrased_question']:
        continue
    src = item['paraphrased_question']
    src = src.replace('?','').replace('-',' - ')
    questionarr = src.split()
    questionftembed = ft(questionarr)
    wikisparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
    ents = re.findall( r'wd:(.*?) ',wikisparql)
    rels = re.findall( r'wdt:(.*?) ',wikisparql)
    rels += re.findall( r'p:(.*?) ',wikisparql)
    rels += re.findall( r'ps:(.*?) ',wikisparql)
    rels += re.findall( r'pq:(.*?) ',wikisparql)
    questionftembed = ft(questionarr)
    strarr = questionarr
    embarr = [x+200*[0.0] for x in questionftembed]
    #[print(x,y) for x,y in zip(questionarr,questionftembed)]
    strarr += ['[SEP]']
    citem['goldentrel'] = ents + rels
    embarr += [500*[-1.0]]
    for cand in citem['goldentrel']:
        cand = cand.replace('}','').replace('.','')
        if not cand:
            continue
        if cand[0] == 'P':
            if cand in props:
                rellabel = props[cand]
                relftembed = ft([rellabel])[0]
            else:
                relftembed = 300*[0.0]
            relkgembed = kgembed(cand)
            embarr += [relftembed+relkgembed]
            strarr += [cand]
            continue
        if cand[0] == 'Q':
            entlabel = getentlabel(cand)
            if entlabel:
                entftembed = ft([entlabel])[0]
            else:
                entftembed = 300*[0.0]
            entkgembed = kgembed(cand)
            embarr += [entftembed+entkgembed]
            strarr += [cand]
            continue
    #print(len(embarr))
    citem['goldentrelvectorstring'] = strarr
    citem['goldentrelvector'] = embarr
    f.write(json.dumps(citem)+'\n')

f.close()
