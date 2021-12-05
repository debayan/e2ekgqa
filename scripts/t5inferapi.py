import logging
import json
import sys
import copy
import torch.multiprocessing
from flask import request, Response
from flask import Flask
from gevent.pywsgi import WSGIServer
torch.multiprocessing.set_sharing_strategy('file_system')
import pandas as pd
from simpletransformers.t5 import T5Model, T5Args

logging.basicConfig(level=logging.INFO)
transformers_logger = logging.getLogger("transformers")
transformers_logger.setLevel(logging.WARNING)

model_args = T5Args()
model_args.use_multiprocessed_decoding = False
model_args.use_multiprocessing = False
model_args.fp16 = False

model = T5Model("t5", "outputs4/checkpoint-11164-epoch-4/", args=model_args)
app = Flask(__name__)

@app.route('/ques2labels', methods=['POST'])
def ques2labels():
    d = request.get_json(silent=True)
    print(d)
    nlquery = d['question']
    preds = model.predict([nlquery])
    return json.dumps({'nlquery':nlquery,'outputlabels':preds[0]}, indent=4)

if __name__ == '__main__':
    http_server = WSGIServer(('', 2222), app)
    http_server.serve_forever()
