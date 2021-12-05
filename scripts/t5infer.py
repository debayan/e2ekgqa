import logging
import json
import sys
import copy
import torch.multiprocessing
torch.multiprocessing.set_sharing_strategy('file_system')
import pandas as pd
from simpletransformers.t5 import T5Model, T5Args

logging.basicConfig(level=logging.INFO)
transformers_logger = logging.getLogger("transformers")
transformers_logger.setLevel(logging.WARNING)

testd = json.loads(open(sys.argv[1]).read())

testbatch = ['rel: '+x['question'] for x in testd]

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

model_args = T5Args()
model_args.use_multiprocessed_decoding = False
model_args.use_multiprocessing = False
model_args.fp16 = False
#model_args.learning_rate = 2e-5


model = T5Model("t5", sys.argv[2], args=model_args)

count = 0
result = []
for batch in list(chunks(testbatch, 32)):
    preds = model.predict(batch)
    for pred in preds:
        item = copy.deepcopy(testd[count])
        item['labelpred'] = pred
        item['labelgold'] = item['labels']
        del item['labels']
        result.append(item)
        print(pred)
        print(testd[count])
        count += 1

f = open(sys.argv[3],'w')
f.write(json.dumps(result,indent=4))
f.close()
