import xml.etree.ElementTree as ET

def generate_record(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)
    
    abstract_text = "" if abstract_text is None else abstract_text
    body_text = "" if body_text is None else body_text
    
    if abstract_text and body_text:
        combined_text = f"# Abstract\n{abstract_text}\n\n# Body\n{body_text}"
    elif abstract_text:
        combined_text = f"# Abstract\n{abstract_text}"
    elif body_text:
        combined_text = f"# Body\n{body_text}"
    else:
        combined_text = ""
    
    return combined_text

def extract_abstract_and_body_text(xml_string):
    try:
        root = ET.fromstring(xml_string)
        abstract_section = root.find('.//front/article-meta/abstract')
        body_section = root.find('.//body')
        abstract_text = xml_to_markdown(ET.tostring(abstract_section, encoding='unicode')) if abstract_section is not None else ""
        body_text = xml_to_markdown(ET.tostring(body_section, encoding='unicode')) if body_section is not None else ""
        return abstract_text, body_text
    except ET.ParseError as e:
        print(f"Failed to parse XML: {e}")
        return "", ""

def safe_text(text):
    return text if text is not None else ""

def xml_to_markdown(xml_string):
    def process_element(element, parent_tag=None):
        md = ""
        if element.tag == 'sec':
            title_element = element.find('title')
            if title_element is not None:
                md += "## " + safe_text(title_element.text) + "\n\n"
            for child in element:
                if child.tag != 'title':
                    md += process_element(child, parent_tag=element.tag)
        elif element.tag == 'title' and parent_tag != 'sec':
            md += "### " + safe_text(element.text) + "\n\n"
        elif element.tag == 'p':
            md += ''.join(element.itertext()).strip() + "\n\n"
        elif element.tag == 'bold':
            md += "**" + safe_text(element.text) + "**"
        elif element.tag == 'italic':
            md += "*" + safe_text(element.text) + "*"
        elif element.tag == 'xref':
            pass
        elif element.tag == 'fig':
            pass
        return md

    root = ET.fromstring(xml_string)
    markdown = ""
    for child in root:
        markdown += process_element(child)

    return markdown.strip()