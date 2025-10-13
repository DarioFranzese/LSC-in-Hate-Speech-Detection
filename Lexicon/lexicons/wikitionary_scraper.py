import requests
from bs4 import BeautifulSoup
import csv
import time
import re

def extract_entry_data(li_element):
    """
    Estrae classes, definition e quotations da un elemento <li>
    """
    # Estrai classes (label-content)
    classes = []
    label_content = li_element.find('span', class_='label-content')
    if label_content:
        for link in label_content.find_all('a'):
            classes.append(link.get_text(strip=True))
    
    classes_str = ', '.join(classes) if classes else ''
    
    # Estrai quotations
    quotations = []
    quote_container = li_element.find('ul', class_='wikt-quote-container')
    if quote_container:
        for quote_li in quote_container.find_all('li', recursive=False):
            quoted_passage = quote_li.find('span', class_='cited-passage')
            if quoted_passage:
                for b_tag in quoted_passage.find_all('b'):
                    b_tag.unwrap()
                quote_text = quoted_passage.get_text(separator=' ', strip=True)
                quotations.append(quote_text)
    
    quotations_str = ' || '.join(quotations) if quotations else ''
    
    # Estrai la definizione
    li_copy = BeautifulSoup(str(li_element), 'html.parser').find('li')
    
    # Rimuovi elementi non necessari
    for tag in li_copy.find_all(['ul', 'dl', 'sup']):
        tag.decompose()
    
    for tag in li_copy.find_all('span', class_='usage-label-sense'):
        tag.decompose()
    
    for tag in li_copy.find_all('span', class_='HQToggle'):
        tag.decompose()
    
    definition = li_copy.get_text(separator=' ', strip=True)
    definition = re.sub(r'\s+', ' ', definition)
    
    if not definition or len(definition) < 3:
        return None
    
    return {
        'classes': classes_str,
        'definition': definition,
        'quotations': quotations_str
    }

def get_word_data(word_url, session):
    """
    Estrae tutte le definizioni dalla pagina della parola
    """
    try:
        response = session.get(word_url, timeout=15)
        if response.status_code != 200:
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Trova la sezione English
        english_heading = soup.find('span', {'id': 'English'})
        if not english_heading:
            return None
        
        all_entries = []
        
        # Trova il parent h2
        parent = english_heading.find_parent('h2')
        if not parent:
            return None
        
        # Itera sui sibling fino alla prossima lingua (h2)
        for sibling in parent.find_next_siblings():
            if sibling.name == 'h2':
                break
            
            # Cerca liste di definizioni (ol)
            for ol in sibling.find_all('ol'):
                for li in ol.find_all('li', recursive=False):
                    entry = extract_entry_data(li)
                    if entry:
                        all_entries.append(entry)
        
        return all_entries if all_entries else None
        
    except Exception as e:
        print(f"  Errore: {e}")
        return None

def process_csv_with_links(input_csv, output_csv='wiktionary_words_complete.csv'):
    """
    Legge CSV con word,link e crea nuovo CSV con word,classes,definition,quotations
    """
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
    })
    
    # Inizializza sessione
    try:
        session.get('https://en.wiktionary.org', timeout=10)
        time.sleep(2)
    except:
        pass
    
    # Leggi il CSV di input
    words_to_process = []
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            words_to_process.append({
                'word': row['word'],
                'link': row['link']
            })
    
    print(f"Trovate {len(words_to_process)} parole da processare\n")
    
    # Apri file di output e scrivi progressivamente
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['word', 'classes', 'definition', 'quotations'])
        writer.writeheader()
        
        total_entries = 0
        
        for idx, word_data in enumerate(words_to_process, 1):
            word = word_data['word']
            link = word_data['link']
            
            print(f"[{idx}/{len(words_to_process)}] {word}...", end=' ')
            
            entries = get_word_data(link, session)
            
            if entries:
                for entry in entries:
                    writer.writerow({
                        'word': word,
                        'classes': entry['classes'],
                        'definition': entry['definition'],
                        'quotations': entry['quotations']
                    })
                    total_entries += 1
                
                f.flush()  # Forza scrittura su disco
                print(f"✓ ({len(entries)} entries)")
            else:
                print("✗ (no entries)")
            
            # Rate limiting
            time.sleep(1.5)
            
            # Pausa più lunga ogni 50 parole
            if idx % 50 == 0:
                print(f"\n--- Pausa (processate {idx} parole) ---")
                time.sleep(5)
    
    print(f"\n=== COMPLETATO ===")
    print(f"Parole processate: {len(words_to_process)}")
    print(f"Totale entries salvate: {total_entries}")
    print(f"File output: {output_csv}")
    
    return total_entries

# Esegui
total = process_csv_with_links('wiktionary_words.csv', 'wiktionary_words_complete.csv')
