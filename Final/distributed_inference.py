import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline
from accelerate import Accelerator
from datasets import load_dataset, Dataset

def get_prompts(texts: list) -> list:

    system_content = (
        "You are an expert digital archivist specializing in late 18th and 19th-century English documents.\n"
        "Your task is to restore text extracted from historical newspapers that has been corrupted by OCR errors.\n"
        "Strict Guidelines:\n"
        "1. **Fix OCR Noise:** Correct obvious scanning errors, broken words (e.g., \"th e\"), and random symbols.\n"
        "3. **Preserve Historical Style:** Do NOT modernize the language. Keep archaic spellings if they are genuine.\n"
        "4. **No Content Change:** Do not summarize, explain, or alter the meaning of the text.\n"
        "5. **Output Format:** Output ONLY the restored text."
    )


    prompts = [
        [
            {"role": "system", "content": system_content},
            {"role": "user", "content": f"Original OCR Text:\n{text}\n\nRestored Text:"}
        ] 
        for text in texts
    ]

    print("PROMPTS ADDEDD")

    return prompts


def save_dataset(dataset: Dataset):
    repo = '/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/Cleaned Old'
    
    dataset.to_parquet(
        f"{repo}/data_old_{accelerator.process_index:05d}.parquet",
        compression='snappy'
    )
    print(f"DATASET FOR PROCESS {accelerator.process_index} SAVED")



if __name__ == "__main__":

    accelerator = Accelerator()

    model_id = "Qwen/Qwen3-4B-Instruct-2507"

    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        dtype=torch.bfloat16,
    )
    tokenizer = AutoTokenizer.from_pretrained(model_id, padding_side = 'left')

    model.to(accelerator.device)

    print("MODEL CREATED AND MOVED TO DEVICE")

    dataset = load_dataset('parquet', data_dir='/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/Old', split='train')

    old_subset = dataset.filter(
        lambda x: int(x['date'][:4]) <= 1850,
        num_proc=64,
    )

    if(accelerator.is_main_process): # Only the main process is going to re-save the newest data
        new_subset = dataset.filter(
            lambda x: int(x['date'][:4]) > 1850,
            num_proc=64,
        )

    dataset = None # Just to free some memory

    print("DATASET LOADED AND SPLIT")


    with accelerator.split_between_processes(old_subset) as split_dataset:

        print(f"First example is {split_dataset['text'][0][:15]}...")
        
        if(accelerator.is_main_process):
            print(f"This is main process and first row is: {new_subset['text'][0][:15]}...")

        prompts = get_prompts(split_dataset['text'])
        
        pipe = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            do_sample=False,
            batch_size=100,
        )

        print("PIPELINE CREATED, READY FOR INFERENCE")

        results = []
        for output in pipe(prompts, max_new_tokens=32):
            results.extend([o[-1]['generated_text'] for o in output])

        print(f"INFERENCE COMPLETED FOR PROCESS {accelerator.process_index}")

        split_dataset = split_dataset.remove_columns(['text'])
        split_dataset = split_dataset.add_column(name = 'text', column =  results)

        save_dataset(split_dataset)

        


    if(accelerator.is_main_process): # the main process will handle the saving of the "newer" part of the dataset (which hasn't been modified)
        repo = '/home/users/industry/cnrsatcreate/farahben/scratch/mydatasets/Cleaned Old'
        
        for i in range(150):
            shard = new_subset.shard(index=i, num_shards=150, contiguous=True)
            shard.to_parquet(
                f"{repo}/data_new_{i+4:05d}.parquet",
                compression='snappy'
            )