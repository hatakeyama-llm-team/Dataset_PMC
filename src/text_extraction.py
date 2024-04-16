import xml.etree.ElementTree as ET
import logging

def generate_record(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)
    
    abstract_text = "" if abstract_text is None else abstract_text
    body_text = "" if body_text is None else body_text
    concat_text = f"{abstract_text}\n\n{body_text}"
    logging.debug(f"🔤 concat_text length: {len(concat_text)}")
    return concat_text

def extract_abstract_and_body_text(xml_string):
    try:
        root = ET.fromstring(xml_string)
        abstract_section = root.find('.//front/article-meta/abstract')
        body_section = root.find('.//body')
        abstract_text = xml_to_plain_text(ET.tostring(abstract_section, encoding='unicode')) if abstract_section is not None else ""
        body_text = xml_to_plain_text(ET.tostring(body_section, encoding='unicode')) if body_section is not None else ""
        return abstract_text, body_text
    except ET.ParseError as e:
        print(f"Failed to parse XML: {e}")
        return "", ""

def safe_text(text):
    return text if text is not None else ""

def xml_to_plain_text(xml_string):
    def process_element(element):
        text = ""
        # xrefとfigタグは無視
        if element.tag == 'xref' or element.tag == 'fig':
            return text
        # secとtitleタグのテキストの後に改行を挿入
        if element.tag == 'sec' or element.tag == 'title':
            text += ''.join(element.itertext()).strip() + "\n"
        # p, bold, italicタグはそのまま
        elif element.tag == 'p' or element.tag == 'bold' or element.tag == 'italic':
            text += ''.join(element.itertext()).strip()
        # 子要素の処理
        for child in element:
            text += process_element(child)
        
        return text

    root = ET.fromstring(xml_string)
    plain_text = ""
    for child in root:
        plain_text += process_element(child)

    return plain_text.strip()