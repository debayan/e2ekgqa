import sys,os,json,requests,re,copy

f = open(sys.argv[1],'w')

d = json.loads(open('train.json').read())
url = 'http://localhost:2221/kgqa'
master = []
correct = 0
pnelcandsuccess = 0
pnelsuccess = 0
pcandsuccess = 0
qcandsuccess = 0
psuccess = 0
qsuccess = 0
for qno,item in enumerate(d):
	print("idx:",qno)
	citem = copy.deepcopy(item)
	if not item['question']:
		continue
	question = item['question'].replace('?','').replace('{','').replace('}','')
	print(question)
	goldsparql = item['sparql_wikidata'].replace('(',' ( ').replace(')',' ) ').replace('{',' { ').replace('}',' } ')
	newvars = ['?vr0','?vr1','?vr2','?vr3','?vr4','?vr5']
	sparql_split = goldsparql.split()
	variables = set([x for x in sparql_split if x[0] == '?'])
	for idx,var in enumerate(sorted(variables)):
		goldsparql = goldsparql.replace(var,newvars[idx])
	ents = re.findall( r'wd:(.*?) ', goldsparql)
	rels = re.findall( r'wdt:(.*?) ',goldsparql)
	rels += re.findall( r'p:(.*?) ',goldsparql)
	rels += re.findall( r'ps:(.*?) ',goldsparql)
	rels+= re.findall( r'pq:(.*?) ',goldsparql)
	r = requests.post("http://localhost:2221/kgqa",json={'question': question},headers={'Connection':'close'})
	result = r.json()
	goldentrelset = set(ents+rels)
	predentrels = result['predentrels']
	predents = [x for x in predentrels if x[0] == 'Q']
	predrels = [x for x in predentrels if x[0] == 'P']
	if set(ents).intersection(set(result['candidatestring'])) == set(ents):
		print("PNEL Q cand CORRECT")
		qcandsuccess += 1
	if set(rels).intersection(set(result['candidatestring'])) == set(rels):
		print("PNEL P cand CORRECT")
		pcandsuccess += 1
	if set(predrels) == set(rels):
		print("PNEL P CORRECT")
		psuccess += 1
	if set(predents) == set(ents):
		print("PNEL Q CORRECT")
		qsuccess += 1
	if goldentrelset.intersection(set(result['candidatestring'])) == goldentrelset:	
		print("PNEL CANDS CORRECT")
		pnelcandsuccess += 1
	if goldentrelset == set(result['predentrels']):
		print("PNEL CORRECT")
		pnelsuccess += 1
	citem['result'] = result
	master.append(citem)
#	citem['api_results'] = result
#	predsparql = result['queries'][0]
#	gs = [x.lower() for x in goldsparql.strip().split()]
#	ps =  [x.lower() for x in predsparql.strip().split()]
#	print("gold sparql:", ' '.join(gs))
#	print("pred sparql:", ' '.join(ps))
	print("goldentrels:", goldentrelset)
	print("predentrels:", set(result['predentrels']))
#	print("predi query:", ' '.join(result['predicted_query']))
#	print("outputlabel:",result['outputlabels'])
#	if gs == ps:
#		print("CORRECT")
#		correct += 1
#	citem['pred_sparqls'] = result['queries']
#	citem['gold_sparql'] = goldsparql
#	citem['goldentrels'] = ents+rels
#	citem['predentrels'] = result['predentrels']
#	master.append(citem)
	print("Accuracy:",correct/float(qno+1.1))
	print("PNEL Cand  Acc:",pnelcandsuccess/float(qno+1.1))
	print("PNEL Acc:",pnelsuccess/float(qno+1.1))
	print("Q cand Acc:",qcandsuccess/float(qno+1.1))
	print("P cand Acc:",pcandsuccess/float(qno+1.1))
	print("Q Acc:",qsuccess/float(qno+1.1))
	print("P Acc:",psuccess/float(qno+1.1))
	if qno % 100 == 0:
		f.write(json.dumps(master,indent=4))
f.close()
