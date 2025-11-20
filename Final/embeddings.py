from datasets import load_dataset, Dataset
from sentence_transformers import SentenceTransformer
import os
import json
import torch

def get_lexicon() -> list:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "data", "apriori_lexicon.json")

    with open(file_path, 'r') as f:
        lexicon = json.load(f)

    return [w['word'].lower() for w in lexicon]


def get_embedding(texts, model, pool):
    embeddings = model.encode(texts, pool=pool, device = ['cuda:0', 'cuda:1', 'cuda:2', 'curda:3'])
    return embeddings.mean(axis=0) # Pooling


if __name__ == "__main__":

    num_gpus = torch.cuda.device_count()
    print(f"Number of available GPUs: {num_gpus}")

    dataset = load_dataset('parquet', data_dir='/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/Modern', split='train')
    print("################### DATASET LOADED  ######################")
    
    lexicon = get_lexicon()
    print("################### LEXICON LOADED  ######################")

    model = SentenceTransformer("Qwen/Qwen3-Embedding-0.6B")
    pool=model.start_multi_process_pool()

    embeddings = []

    for word in lexicon:
        examples = dataset.filter(
            lambda example, w=word: example['word'].lower() == w,
            num_proc=64
        )
        embeddings.append(get_embedding(examples['text'], model, pool))

    print("################### EMBEDDINGS GENERATION COMPLETED  ######################")

    model.stop_multi_process_pool(pool)

    embeddings_dataset = Dataset.from_dict({'word': lexicon, 'embedding': embeddings})
    print(f"################### DATASET OF EMBEDDINGS CREATED WITH LENGHT {len(embeddings_dataset)} ######################")

    dataset.to_parquet('/home/users/industry/cnrsatcreate/farahben/scratch/myembeddings/modern_embeddings.parquet')
    print("################### EMBEDDINGS SAVED ######################")


