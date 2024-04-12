from text_extraction import extract_abstract_and_body_text

def analyze_text_sentences(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)

    # Replace None with empty strings if necessary
    abstract_text = "" if abstract_text is None else abstract_text
    body_text = "" if body_text is None else body_text

    # Construct the combined text based on content availability
    if abstract_text and body_text:
        combined_text = f"# Abstract\n{abstract_text}\n\n# Body\n{body_text}"
    elif abstract_text:
        combined_text = f"# Abstract\n{abstract_text}"
    elif body_text:
        combined_text = f"# Body\n{body_text}"
    else:
        combined_text = ""
    
    return combined_text
