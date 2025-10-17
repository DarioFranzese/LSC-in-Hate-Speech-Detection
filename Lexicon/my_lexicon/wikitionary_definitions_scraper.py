from bs4 import BeautifulSoup
import requests
import json

def get_wiktionary_definitions(word):
    """
    Extract structured definitions from Wiktionary.
    
    Args:
        word: The word to search for
        
    Returns:
        List of dictionaries, each containing:
            - pos (str): Part of speech, either "Noun" or "Adjective"
            - tags (list of str): Filtered list of tags from ["derogatory", "vulgar", "slang", "offensive"]
            - description (str): Clean text description without HTML tags or links
            - quotations (list of str): List of quotations in format "year; citation text"
        Returns empty list if:
            - English section is not found on the page
            - No Noun or Adjective definitions are present
    """
    
    url = f"https://en.wiktionary.org/wiki/{word}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive'
    }
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    html_content = response.text
    
    soup = BeautifulSoup(html_content, 'html.parser')
    definitions = []
    
    # Find the English section
    english_h2 = soup.find('h2', id='English')
    
    # Return empty list if English section not found
    if not english_h2:
        return definitions
    
    # Find the next h2 (another language section)
    next_h2 = english_h2.find_next('h2')
    
    # Search for both h3 and h4 with id "Noun" or "Adjective" in the English section
    current = english_h2
    
    while current:
        if current == next_h2:
            break
        
        # Find the next h3 or h4
        next_h3 = current.find_next('h3')
        next_h4 = current.find_next('h4')
        
        # Determine which comes first
        if next_h3 and next_h4:
            if next_h3.sourceline and next_h4.sourceline:
                current = next_h3 if next_h3.sourceline < next_h4.sourceline else next_h4
            else:
                current = next_h3
        elif next_h3:
            current = next_h3
        elif next_h4:
            current = next_h4
        else:
            break
        
        # Stop if we're beyond the English section
        if next_h2 and current.sourceline and next_h2.sourceline and current.sourceline > next_h2.sourceline:
            break
        
        pos_id = current.get('id', '')
        
        # Filter only Noun and Adjective (both as h3 and h4)
        if pos_id in ['Noun', 'Adjective'] or pos_id.startswith('Noun_') or pos_id.startswith('Adjective_'):
            pos_normalized = 'Noun' if pos_id.startswith('Noun') else 'Adjective'
            
            # Find the ol that follows
            sibling = current.parent.find_next_sibling()
            
            while sibling and sibling.name != 'ol':
                sibling = sibling.find_next_sibling()
                if sibling and sibling.name == 'div' and sibling.find('h2'):
                    break
            
            if sibling and sibling.name == 'ol':
                ol = sibling
                
                for li in ol.find_all('li', recursive=False):
                    definition_obj = {
                        'pos': pos_normalized,
                        'tags': [],
                        'description': '',
                        'quotations': []
                    }
                    
                    # Extract tags
                    usage_label = li.find('span', class_='usage-label-sense')
                    if usage_label:
                        label_content = usage_label.find('span', class_='ib-content')
                        if label_content:
                            text_content = label_content.get_text().lower()
                            for tag in ['derogatory', 'vulgar', 'slang', 'offensive']:
                                if tag in text_content:
                                    definition_obj['tags'].append(tag)
                    
                    # Extract quotations
                    for citation_whole in li.find_all('div', class_='citation-whole'):
                        # Year from cited-source
                        year = ''
                        cited_source = citation_whole.find('span', class_='cited-source')
                        if cited_source:
                            year_tag = cited_source.find('b')
                            if year_tag:
                                year = year_tag.get_text().strip()
                        
                        # Citation from h-quotation > e-quotation
                        citation_text = ''
                        h_quotation = citation_whole.find('div', class_='h-quotation')
                        if h_quotation:
                            citation_passage = h_quotation.find('span', class_='e-quotation')
                            if citation_passage:
                                citation_text = citation_passage.get_text().strip()
                                citation_text = ' '.join(citation_text.split())
                        
                        if year or citation_text:
                            quotation = f"{year}; {citation_text}" if year else citation_text
                            definition_obj['quotations'].append(quotation)
                    
                    # Extract description
                    li_copy = BeautifulSoup(str(li), 'html.parser').find('li')
                    
                    # Remove unwanted elements
                    for elem in li_copy.find_all('span', class_='usage-label-sense'):
                        elem.decompose()
                    for elem in li_copy.find_all('span', class_='nyms-toggle'):
                        elem.decompose()
                    for elem in li_copy.find_all('span', class_='HQToggle'):
                        elem.decompose()
                    for elem in li_copy.find_all('dl'):
                        elem.decompose()
                    for elem in li_copy.find_all('ul'):
                        elem.decompose()
                    
                    # Extract clean text
                    description_text = li_copy.get_text().strip()
                    description_text = ' '.join(description_text.split())
                    
                    definition_obj['description'] = description_text
                    
                    definitions.append(definition_obj)
    
    return definitions