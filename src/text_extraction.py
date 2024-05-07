import xml.etree.ElementTree as ET

import re

def remove_vacant_brackets(text):
    text = re.sub(r'\[(?![^\[\]]*[a-zA-Z])[^\[\]]*\]', '', text)
    text = re.sub(r'\((?![^\(\)]*[a-zA-Z])[^\(\)]*\)', '', text)
    return text

def generate_record(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)
    return f"{abstract_text}\n\n{body_text}".strip()

def extract_abstract_and_body_text(xml_string):
    try:
        root = ET.fromstring(xml_string)
        abstract_section = root.find('.//front/article-meta/abstract')
        body_section = root.find('.//body')
        abstract_text = (ET.tostring(abstract_section, encoding='unicode')) if abstract_section is not None else ""
        body_text = xml_to_clean_plain_text(ET.tostring(body_section, encoding='unicode'),['xref','fig','table-wrap','inline-formula','disp-formula']) if body_section is not None else ""
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
            text += " ".join(element.itertext()).strip() + "\n"
        # p, bold, italicタグはそのまま
        elif element.tag == 'p' or element.tag == 'bold' or element.tag == 'italic':
            text += " ".join(element.itertext()).strip()
        # 子要素の処理
        for child in element:
            text += process_element(child)

        return text

    root = ET.fromstring(xml_string)
    plain_text = ""
    for child in root:
        plain_text += process_element(child)

    return plain_text.strip()

def xml_to_clean_plain_text(xml_string,remove_list):
    # 削除するタグ一覧
    # xref
    # fig
    # table-wrap
    # inline-formula
    # disp-formula
    
    def process_element(element,remove_list):
        text = ""
        if element.tag == 'xref':                
            return text
        elif  element.tag == 'fig':            
            return text
        elif  element.tag == 'inline-formula' :                  
            return text
        elif  element.tag == 'disp-formula':
            return text
        elif   element.tag == 'table-wrap':            
            return text
        else :
            text += " ".join(update_itertext(element,remove_list)).strip()            

        return text

    root = ET.fromstring(xml_string)
    plain_text = ""
    for child in root:
        plain_text += process_element(child,remove_list)

    return remove_vacant_brackets(plain_text.strip().replace("\n", " ") )

def update_itertext(self,remove_list):
    """Create text iterator.

    The iterator loops over the element and all subelements in document
    order, returning all inner text.

    """
    tag = self.tag
    if not isinstance(tag, str) and tag is not None:
        return
    t = self.text
    if t:
        yield t
    for e in self:
		    # 取り除きたいタグのリストで選別
        if not e.tag in remove_list:
            yield from update_itertext(e,remove_list)
        t = e.tail
        if t:                
            yield t