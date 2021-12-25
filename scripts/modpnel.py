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
        if len(citem['vector']) == 0:
            continue
        count = 0
        qemb = citem['vector'][0][:768]
        for token,vector in zip(citem['vectorstring'],citem['vector']):
            count += 1
            if count %10 == 0:
                candidatevectors.append([qemb+200*[0.0],'[QEMB]',0.0])
            if token in goldents or token in goldrels:
                candidatevectors.append([vector[768:],token,1.0])
            else:
                candidatevectors.append([vector[768:],token,0.0])
        del citem['vector']
        f.write(json.dumps([uid,goldents+goldrels,candidatevectors,citem])+'\n')
f.close()
