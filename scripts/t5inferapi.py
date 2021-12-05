import logging,os
import json
import sys
import torch.multiprocessing
from flask import request, Response
from flask import Flask
from gevent.pywsgi import WSGIServer
torch.multiprocessing.set_sharing_strategy('file_system')
import pandas as pd
from simpletransformers.t5 import T5Model, T5Args
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer, util
import Vectoriser

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
transformers_logger = logging.getLogger("transformers")
transformers_logger.setLevel(logging.WARNING)

model_args = T5Args()
model_args.use_multiprocessed_decoding = False
model_args.use_multiprocessing = False
model_args.fp16 = False

model = T5Model("t5", "outputs4/checkpoint-11164-epoch-4/", args=model_args)

sentencemodel = SentenceTransformer('all-MiniLM-L6-v2')

reldict = json.loads(open('en.json').read())
goldrellabels = []

for k,v in reldict.items():
    goldrellabels.append([k,v])


sentence_embeddings = sentencemodel.encode([x[1] for x in goldrellabels], convert_to_tensor=True)

es = Elasticsearch(host='ltcpu1',port=49158)
print("api ready ...")

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



@app.route('/ques2labels', methods=['POST'])
def ques2labels():
    d = request.get_json(silent=True)
    print(d)
    nlquery = d['question']
    preds = model.predict([nlquery])
    pred = preds[0]
    x,y = Vectoriser.vectorise(nlquery,pred)
    return json.dumps({'nlquery':nlquery,'outputlabels':pred, 'candidatestring': x, 'candidatevectors': y}, indent=4)

if __name__ == '__main__':
    http_server = WSGIServer(('', 2222), app)
    http_server.serve_forever()
