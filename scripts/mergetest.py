import sys,os,json,copy

dpred = json.loads(open(sys.argv[1]).read())
dgold = json.loads(open(sys.argv[2]).read())


preddict = {}
for preditem in dpred:
    if preditem['uid'] not in preddict:
        preddict[preditem['uid']] = preditem

print(preddict)

goldarr = []
for item in dgold: 
    uid = item['uid']
    citem = copy.deepcopy(item)
    try:
        citem['goldlabel'] = preddict[uid]['labelgold']
    except Exception as err:
        citem['goldlabel'] = []
    try:
        citem['predlabel'] = preddict[uid]['labelpred']
    except Exception as err:
        citem['predlabel'] = []
    goldarr.append(citem)

f = open(sys.argv[3],'w')
f.write(json.dumps(goldarr,indent=4))
f.close()
        
