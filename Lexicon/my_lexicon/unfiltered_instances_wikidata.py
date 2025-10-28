import json
import csv
from tqdm import tqdm

# Percorsi dei file
input_file = "full.json" # quello filtered parte da base_filtering (una versione vecchia), attenzione perché effettua comunque un filtro quindi "unfiltered" puó essere ingannevole ma si riferisce al lexicon di partenza
output_file = "unfiltered_instances_wikidata.csv"

# Carica il JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# Prepara il CSV
with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["word", "description", "year", "text"])  # intestazioni

    
    for entry in tqdm(data, desc="Processing words", unit="word"):
        word = entry.get("word", "")
        for definition in entry.get("definitions", []):
            # Considera solo le definitions con tags non vuoto
            if definition.get("tags"):
                description = definition.get("description", "")
                for quote in definition.get("quotations", []):
                    if isinstance(quote, str) and len(quote) >= 6:
                        year = quote[:4]
                        text = quote[6:]
                        writer.writerow([word, description, year, text])

print(f"✅ File '{output_file}' generato con successo!")
