import sys,os,json,copy,torch
from elasticsearch import Elasticsearch

from sentence_transformers import SentenceTransformer, util
model = SentenceTransformer('all-MiniLM-L6-v2')

reldict = json.loads(open('en.json').read())
goldrellabels = []

for k,v in reldict.items():
    goldrellabels.append([k,v])


sentence_embeddings = model.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

dgold = json.loads(open(sys.argv[1]).read())
es = Elasticsearch(host='ltcpu1',port=49158)


def relcands(rellabel):
    results = []
    query = rellabel.strip()
    if not query:
        return []
    query_embedding = model.encode(query, convert_to_tensor=True)

    # We use cosine-similarity and torch.topk to find the highest 5 scores
    cos_scores = util.pytorch_cos_sim(query_embedding, sentence_embeddings)[0]
    top_results = torch.topk(cos_scores, k=30)

    #print("\n\n======================\n\n")
    #print("Query:", query)
    #print("\nTop 5 most similar sentences in corpus:")

    for score, idx in zip(top_results[0], top_results[1]):
        #print(goldrellabels[idx], "(Score: {:.4f})".format(score))
        results.append(goldrellabels[idx])
    return results

def entcands(entlabel):
    esresults = es.search(index='wikidataentitylabelindex02',body={"query":{"match":{"wikidataLabel":entlabel}}},size=30)
    results = []
    try:
        for res in esresults['hits']['hits']:
            #print(entlabel, res['_source'])
            results.append(res['_source'])
        return results
    except Exception as err:
        print(entlabel,err)
        return results

goldarr = []
for idx,item in enumerate(dgold): 
    citem = copy.deepcopy(item)
    uid = item['uid']
    predlabels = item['predlabel']
    goldlabels = item['goldlabel']
    entss = ''
    relss = ''
    try:
        entss,relss = predlabels.split('//')
    except Exception as err:
        print(err, predlabels)
        entss = predlabels
    try:
        ents = entss.split('::')
        rels = relss.split(';;')
    except Exception as err:
        print(err)
        continue
    print(idx)
    print("goldlabels:", goldlabels)
    print("predlabels:",predlabels)
    print("ents      :",ents)
    print("rels      :",rels)
    citem['entlabelcands'] = {}
    citem['rellabelcands'] = {}
    for ent in ents:
        results = entcands(ent)
        citem['entlabelcands'][ent] = results
    for rel in rels:
        results = relcands(rel)
        citem['rellabelcands'][rel] = results
    goldarr.append(citem)

f = open(sys.argv[2],'w')
f.write(json.dumps(goldarr,indent=4))
f.close()
