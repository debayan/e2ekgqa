import sys,os,json,copy,re
from elasticsearch import Elasticsearch


reldict = json.loads(open('en.json').read())
goldrellabels = []

for k,v in reldict.items():
    goldrellabels.append([k,v])


d = json.loads(open(sys.argv[1]).read())
es = Elasticsearch(host='ltcpu1',port=49158)


def getentlabel(ent):
    results = es.search(index='wikidataentitylabelindex02',body={"query":{"term":{"uri":{"value":ent}}}})
    try:
        for res in results['hits']['hits']:
            return  res['_source']['wikidataLabel']
    except Exception as err:
        print(results)
        print(ent,err)
        return ''

def getrellabel(rel):
    try:
        return reldict[rel]
    except Exception as err:
        return ''
newvars = ['?vr0','?vr1','?vr2','?vr3','?vr4','?vr5']
marr = []
for item in d:
    try:
        citem = copy.deepcopy(item)
        sparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
        sparql_split = sparql.split()
        print(sparql)
        variables = set([x for x in sparql_split if x[0] == '?'])
        print(variables)
        for idx,var in enumerate(variables):
            sparql = sparql.replace(var,newvars[idx])
        print(sparql)
   #     newitem['sparql_wikidata_reduced_vars'] = sparql
        unit = {}
        unit['uid'] = item['uid']
        unit['question'] = item['question']
        unit['paraphrased_question'] = item['paraphrased_question']
        ents = re.findall( r'wd:(.*?) ',sparql)
        rels = re.findall( r'wdt:(.*?) ',sparql)
        rels += re.findall( r'p:(.*?) ',sparql)
        rels += re.findall( r'ps:(.*?) ',sparql)
        rels += re.findall( r'pq:(.*?) ',sparql)
        modq = []
        entrellabels = []
        for token in sparql.split():
            cleantoken = token.replace('wd:','').replace('wdt:','').replace('p:','').replace('ps:','').replace('pq:','')
            if cleantoken in ents:
                entlabel = getentlabel(cleantoken)
                modq.append(cleantoken)
                entrellabels.append(cleantoken+' '+entlabel)
                continue
            if cleantoken in rels:
                rellabel = getrellabel(cleantoken)
                modq.append(cleantoken)
                entrellabels.append(cleantoken+' '+rellabel)
                continue
            modq.append(cleantoken)
        print(item['sparql_wikidata'])
        print(' '.join(modq))
        try:
            citem['t5concatsparql'] = ' '.join(modq)
            citem['t5concatquestion'] = item['question'] + ' [SEP] ' + ' [SEP] '.join(entrellabels)
            citem['t5concatparaphrasequestion'] = item['paraphrased_question'] + ' [SEP] ' + ' [SEP] '.join(entrellabels)
            citem['t5sparql'] = ' '.join(modq)
            marr.append(citem)
        except Exception as err:
            print(err)
        print(json.dumps(citem,indent=4))
    except Exception as err:
        print(err)

f = open(sys.argv[2],'w')
f.write(json.dumps(marr,indent=4))
f.close() 


