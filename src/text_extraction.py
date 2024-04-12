import xml.etree.ElementTree as ET

def extract_abstract_and_body_text(xml_string):
    try:
        root = ET.fromstring(xml_string) 
        # Extract abstract and body sections
        abstract_section = root.find('.//front/article-meta/abstract')
        body_section = root.find('.//body')
        
        # Convert extracted sections to Markdown
        abstract_text = xml_to_markdown(ET.tostring(abstract_section, encoding='unicode')) if abstract_section is not None else ""
        body_text = xml_to_markdown(ET.tostring(body_section, encoding='unicode')) if body_section is not None else ""

        return abstract_text, body_text
    except ET.ParseError as e:
        print(f"Failed to parse XML: {e}")
        return "", ""

def xml_to_markdown(xml_string):
    def process_element(element, parent_tag=None):
        md = ""
        if element.tag == 'sec':
            title_element = element.find('title')
            if title_element is not None:
                md += "## " + title_element.text + "\n\n"
            for child in element:
                if child.tag != 'title':
                    md += process_element(child, parent_tag=element.tag)
        elif element.tag == 'title' and parent_tag != 'sec':
            md += "### " + element.text + "\n\n"
        elif element.tag == 'p':
            md += ''.join(element.itertext()).strip() + "\n\n"
        elif element.tag == 'bold':
            md += "**" + (element.text or '') + "**"
        elif element.tag == 'italic':
            md += "*" + (element.text or '') + "*"
        elif element.tag == 'xref':
            pass
        elif element.tag == 'fig':
            pass
        return md

    # Parse the XML string
    root = ET.fromstring(xml_string)
    markdown = ""
    for child in root:
        markdown += process_element(child)
    
    return markdown.strip()  # Remove leading/trailing whitespace
