import sys,os,json,re,operator
from elasticsearch import Elasticsearch

es = Elasticsearch(host='ltcpu1',port=49158)
propdict = json.loads(open('../scripts/en.json').read())
d = json.loads(open('test.json').read())
entlabfound = 0
entembfound = 0
relembfound = 0
rellabfound = 0
counter = {}

enttotal = 0
reltotal = 0

def inlabes(ent):
	res = es.search(index="wikidataentitylabelindex02", body={"query":{"term":{"uri":{"value":ent}}}})
	if len(res['hits']['hits']) > 0:
		return True
	else:
		print(ent, res['hits']['hits'])
		return False

def inembes(ent):
	enturl = '<http://www.wikidata.org/entity/'+ent+'>'
	res = es.search(index="wikidataembedsindex01", body={"query":{"term":{"key":{"value":enturl}}}})
	if len(res['hits']['hits']) > 0:
		return True
	else:
		print(ent,res['hits']['hits'])
		return False

for item in d:
	s = item['sparql_wikidata'].replace('{',' { ').replace('}',' } ').replace('.',' . ')
	ents = re.findall( r'wd:(.*?) ', s)
	rels = re.findall( r'wdt:(.*?) ',s)
	rels += re.findall( r'p:(.*?) ', s)
	rels += re.findall( r'ps:(.*?) ',s)
	rels += re.findall( r'pq:(.*?) ', s)
	enttotal += len(ents)
	reltotal += len(rels)
	for ent in ents:
		pass
#		if inlabes(ent):
#			entlabfound += 1
#		if inembes(ent):
#			entembfound += 1
	for rel in rels:
		if rel not in counter:
			counter[rel] = 0
		counter[rel] += 1
#		if  inembes(rel):
#			relembfound += 1
#		if rel in propdict:
#			rellabfound += 1
#	print("entlabfound: %d/%d"%(entlabfound,enttotal))
#	print("entembfound: %d/%d"%(entembfound,enttotal))
#	print("rellabfound: %d/%d"%(rellabfound,reltotal))
#	print("relembfound: %d/%d"%(relembfound,reltotal))
sorted_d = dict( sorted(counter.items(), key=operator.itemgetter(1),reverse=True))
print('Dictionary in descending order by value : ',sorted_d)
for k,v in sorted_d.items():
	print(k,propdict[k])
