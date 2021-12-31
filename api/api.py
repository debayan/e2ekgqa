#!/usr/bin/python

from flask import request, Response
from flask import Flask
from gevent.pywsgi import WSGIServer
import json,sys,requests,logging
from QueryProcessor import QueryProcessor
import copy

app = Flask(__name__)
qp = QueryProcessor()

@app.route('/kgqa', methods=['POST'])
def kgqa():
    d = request.get_json(silent=True)
    print(d)
    nlquery = d['question']
    # 1. query to labels
    r = requests.post("http://localhost:2222/ques2labels",json=d,headers={'Connection':'close'})
    labels = r.json()
    #print("labels:",labels)
    # 2. labels to candidates
    r = requests.post("http://localhost:2223/erlinker",json=labels,headers={'Connection':'close'})
    entrelcands = r.json()
    #print("entrelcands:",entrelcands)
#     3. linked ent rel to query
    r = requests.post("http://localhost:2224/generatequery",json=entrelcands,headers={'Connection':'close'})
    query = r.json()
    #print("query:",query)
	# 4. hit sparql to wikidata and fetch answer
    valid_queries = qp.fetchanswer(query['predicted_query'])
    resultitem = copy.deepcopy(query)
    resultitem['valid_queries'] = valid_queries
    del resultitem['predicted_query']
    del resultitem['linkedentrelstring']
    del resultitem['candidatestring']
    print("results:",json.dumps(resultitem,indent=4))
    return json.dumps(resultitem, indent=4)

print("listening...")
if __name__ == '__main__':
    http_server = WSGIServer(('', 2221), app)
    http_server.serve_forever()
