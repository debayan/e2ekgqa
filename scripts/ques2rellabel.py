import sys,os,json,re

d = json.loads(open(sys.argv[1]).read())
props = json.loads(open('en.json').read())
arr = []

for item in d:
	wikisparql = item['sparql_wikidata']
	unit = {}
	unit['uid'] = item['uid']
	unit['question'] = item['question']
	unit['paraphrased_question'] = item['paraphrased_question']
	rels = re.findall( r'wdt:(.*?) ',wikisparql)
	labelarr = []
	for rel in rels:
		try:
			label = props[rel]
			labelarr.append(label)
			print(label)
		except Exception as err:
			print(err)
			continue
	labelarr.sort()
	if not item['question']:
		continue
	arr.append({"question":item['question'], 'relations': ' :: '.join(labelarr) , 'uid': item['uid']})
	if not item['paraphrased_question']:
		continue
	arr.append({"question":item['paraphrased_question'], 'relations': ' :: '.join(labelarr) , 'uid': item['uid']})

f = open(sys.argv[2],'w')
f.write(json.dumps(arr, indent=4))
f.close()


