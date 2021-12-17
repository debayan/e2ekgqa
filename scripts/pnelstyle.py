import sys,os,json,copy,re

f = open(sys.argv[2],'w')

with open(sys.argv[1]) as infile:
    for line in infile:
        item = json.loads(line.strip())
        citem = copy.deepcopy(item)
        uid = citem['uid']
        sparql = citem['sparql_wikidata']
        goldents = re.findall( r'wd:(.*?) ', sparql)
        goldrels = re.findall( r'wdt:(.*?) ', sparql)
        goldrels += re.findall( r'p:(.*?) ', sparql)
        goldrels += re.findall( r'ps:(.*?) ', sparql)
        goldrels += re.findall( r'pq:(.*?) ', sparql)
        candidatevectors = []
        for token,vector in zip(citem['vectorstring'],citem['vector']):
            if token in goldents or token in goldrels:
                candidatevectors.append([vector,token,1.0])
            else:
                candidatevectors.append([vector,token,0.0])
        del citem['vector']
        f.write(json.dumps([uid,goldents+goldrels,candidatevectors,citem])+'\n')
f.close()
