import sys,os,json,re
from elasticsearch import Elasticsearch
from simpletransformers.t5 import T5Model, T5Args

model_args = T5Args()
model_args.use_multiprocessed_decoding = False
model_args.use_multiprocessing = False
model_args.fp16 = False

t5model = T5Model("t5", "outputs4/checkpoint-11164-epoch-4/", args=model_args)


d = json.loads(open(sys.argv[1]).read())
props = json.loads(open('en1.json').read())
arr = []
es = Elasticsearch(host='ltcpu1',port=49158)

def getlabel(ent):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        for res in results['hits']['hits']:
            return res['_source']['wikidataLabel']
    except Exception as err:
        print(results)
        print(ent,err)
        return ''

for item in d:
    wikisparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
    unit = {}
    unit['uid'] = item['uid']
    unit['question'] = item['question']
    if not item['question']:
        continue
    t5preds = t5model.predict([item['question']])
    t5pred = t5preds[0]
    print(t5pred)
    #unit['paraphrased_question'] = item['paraphrased_question']
    ents = re.findall( r'wd:(.*?) ',wikisparql)
    rels = re.findall( r'wdt:(.*?) ',wikisparql)
    rels += re.findall( r'p:(.*?) ',wikisparql)
    rels += re.findall( r'ps:(.*?) ',wikisparql)
    rels += re.findall( r'pq:(.*?) ',wikisparql)
    entlabelarr = []
    for ent in ents:
        try:
            label = getlabel(ent)
            if not label:
                continue
            entlabelarr.append(label)
        except Exception as err:
            print(err)
            continue
    #entlabelarr.sort()
    rellabelarr = []
    for rel in rels:
        try:
            label = props[rel]
            if not label:
                continue
            rellabelarr.append(label)
        except Exception as err:
            print(err)
            continue
    #rellabelarr.sort()
    if not item['question']:
        continue
    arr.append({"question":item['question'], 'labels': t5pred , 'uid': item['uid'] ,'ents':ents, 'rels': rels, 'sparql_wikidata':wikisparql})

f = open(sys.argv[2],'w')
f.write(json.dumps(arr, indent=4))
f.close()
