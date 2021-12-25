import logging
import json
import sys
import torch.multiprocessing
torch.multiprocessing.set_sharing_strategy('file_system')
import pandas as pd
from simpletransformers.t5 import T5Model, T5Args

logging.basicConfig(level=logging.INFO)
transformers_logger = logging.getLogger("transformers")
transformers_logger.setLevel(logging.WARNING)

traind_ = json.loads(open(sys.argv[1]).read())
x = len(traind_)
traind = traind_[:int(0.95*x)]
vald   = traind_[int(0.95*x):]
print(len(vald))
sys.exit(1)


train_data = [["rel", x['question'], x['labels']] for x in traind]

eval_data = [["rel", x['question'], x['labels']] for x in vald]

train_df = pd.DataFrame(train_data)
train_df.columns = ["prefix", "input_text", "target_text"]

eval_df = pd.DataFrame(eval_data)
eval_df.columns = ["prefix", "input_text", "target_text"]

model_args = T5Args()
model_args.num_train_epochs = 10
model_args.evaluate_generated_text = True
model_args.evaluate_during_training = True
model_args.use_multiprocessed_decoding = False
model_args.use_multiprocessing = False
model_args.train_batch_size = 16
model_args.fp16 = False
model_args.optimizer = 'Adafactor'
model_args.evaluate_during_training_steps = -1

#model_args.learning_rate = 2e-5
model_args.output_dir = 'outputs6/'


model = T5Model("t5", "t5-base", args=model_args)


def count_matches(labels, preds):
    print(labels)
    print(preds)
    return sum([1 if label == pred else 0 for label, pred in zip(labels, preds)])


model.train_model(train_df, eval_data=eval_df, matches=count_matches)

print(model.eval_model(eval_df, matches=count_matches))
