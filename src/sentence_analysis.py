from text_extraction import extract_abstract_and_body_text

def analyze_text_sentences(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)
    
    ## abstract_textがある && body_textがある → # Abstract \n {abstract_text} \n # Body \n {body_text}
    ## abstract_textがある && body_textがない → # Abstract \n {abstract_text}
    ## abstract_textがない && body_textがある → # Body \n {body_text}
    ## abstract_textがない && body_textがない → 空文字列
    if abstract_text and body_text:
        combined_text = f"# Abstract\n{abstract_text}\n\n# Body\n{body_text}"
    elif abstract_text:
        combined_text = f"# Abstract\n{abstract_text}"
    elif body_text:
        combined_text = f"# Body\n{body_text}"
    else:
        combined_text = ""
    
    return combined_text
