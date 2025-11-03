from datasets import load_dataset, Dataset, concatenate_datasets
import pandas as pd
import json
import re
import os


def get_lexicon() -> set:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, "data", "bcadz_filtering.json")

    with open(file_path, 'r') as f:
        lexicon = json.load(f)

    return set(w['word'].lower() for w in lexicon)


def preprocess_dataset(dataset: Dataset) -> Dataset:
    dataset = dataset.select_columns(['date', 'article'])
    dataset.map(
            lambda batch: {
                'article': [
                    text.replace('-\n', '').replace('\n', ' ') 
                    for text in batch['article']
                ]
            },

        num_proc = 128,
        batched = True,
        batch_size = 4000
    )

    return dataset


def save_dataset(dataset: Dataset):
    #repo = '/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/FilteredAmericanStories'
    repo = '/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/LAS'
    num_shards = 50

    for i in range(num_shards):
        shard = dataset.shard(index=i, num_shards=num_shards, contiguous=True)
        shard.to_parquet(
            f"{repo}/data_{i:05d}.parquet",
            compression='snappy'
        )


def get_context(batch):
    dates = []
    target_words = []
    texts = []

    pattern = re.compile(
        r'\b(' + '|'.join(re.escape(word) for word in lexicon) + r')\b',
        re.IGNORECASE
    )
    
    for date, article in zip(batch['date'], batch['article']):
        sentences = [s.strip() for s in article.split('.') if s.strip()]
        
        for idx, sentence in enumerate(sentences):
            # Trova tutte le parole del lexicon nella frase
            matches = pattern.findall(sentence)
            
            if matches:
                # Deduplica matches
                for word in set(match.lower() for match in matches):
                    start_idx = max(0, idx - 1)
                    end_idx = min(len(sentences), idx + 2)
                    context = '. '.join(sentences[start_idx:end_idx])

                    if not context.endswith('.'):
                        context += '.'
                    
                    dates.append(date)
                    target_words.append(word)
                    texts.append(context)
    
    return {'date': dates, 'word': target_words, 'text': texts}


if __name__ == "__main__":

    # dataset = concatenate_datasets(list(load_dataset("davanstrien/AmericanStories-parquet").values()))
    
    
    dataset = load_dataset('parquet', data_dir='/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/FilteredAmericanStories', split='train')
    dataset = preprocess_dataset(dataset)
    
    print("################### DATASET LOADED  ######################")

    lexicon = get_lexicon()
    print("################### LEXICON LOADED  ######################")


    new_dataset = dataset.map(
        get_context, # mapping function
        batched=True,
        batch_size=2000,
        num_proc=128,
        remove_columns=dataset.column_names # lascia solo quelle nuove
    )

    print("################### NEW DATASET CREATED  ######################")

    save_dataset(new_dataset)
    print("################### NEW DATASET SAVED  ######################")



