import re
import lxml.etree as ET


def remove_xref_and_wrapped_brackets(text, proximity=30):
    original_text = text  # オリジナルのテキストを保存

    # 複数行にわたるxrefタグに対応するための正規表現パターン
    xref_pattern = r'<xref[^<]*<\/xref>'
    
    # 余分な空白を整理
    text = re.sub(r'\s+', ' ', text)
    
    # すべての <xref>...</xref> の位置を見つける
    xref_matches = list(re.finditer(xref_pattern, text, re.DOTALL))
    
    # マッチしたものを後ろから処理していく
    for match in reversed(xref_matches):
        start, end = match.span()
        
        # 前後 proximity 文字の範囲を確認
        pre_start = max(0, start - proximity)
        post_end = min(len(text), end + proximity)
        
        # 前後のテキストを抽出
        pre_text = text[pre_start:start]
        post_text = text[end:post_end]
        
        # カッコを探し、xrefタグが含まれているか確認
        pre_bracket_match = re.search(r'(\(|\[)[^\(\)\[\]]*$', pre_text)
        post_bracket_match = re.search(r'^[^\(\)\[\]]*(\)|\])', post_text)
        
        # 対応するカッコを探す
        if pre_bracket_match and post_bracket_match:
            pre_bracket = pre_bracket_match.group(1)
            post_bracket = post_bracket_match.group(1)
            
            # カッコが対応している場合にのみ削除
            if (pre_bracket, post_bracket) in [('(', ')'), ('[', ']')]:
                pre_cut_point = pre_start + pre_bracket_match.start()
                post_cut_point = end + len(post_text) - len(post_bracket_match.group(0)) + post_bracket_match.end() - 1
                
                # カッコとxrefタグを含むテキストを削除
                text = text[:pre_cut_point] + text[post_cut_point:]
        else:
            # 前後にカッコがない場合、xrefタグのみを削除
            text = text[:start] + text[end:]
    
    # 正しいXMLかどうかをチェック
    try:
        ET.fromstring(f"<root>{text}</root>")  # ルート要素を追加してパースを試みる
        return text.strip()
    except ET.XMLSyntaxError:
        return original_text.strip()  # パースエラーの場合、オリジナルのテキストを返す


def extract_abstract_and_body_text(xml_string):
    # Clean up XML string by removing <xref> tags and surrounding brackets
    cleaned_xml_string = remove_xref_and_wrapped_brackets(xml_string)
    
    try:
        # Parse the cleaned XML string using a fault-tolerant parser
        parser = ET.XMLParser(recover=True)
        root = ET.fromstring(cleaned_xml_string, parser=parser)
    except ET.ParseError as e:
        print(f"Failed to parse XML: {e}")
        return "", ""
    
    # Use XPath to extract all text within the <abstract> and <body> sections
    abstract_text = ' '.join(root.xpath('.//front/article-meta/abstract//text()'))
    body_text = ' '.join(root.xpath('.//body//text()'))
    
    # Normalize whitespace
    abstract_text = re.sub(r'\s{2,}', ' ', abstract_text).strip()
    body_text = re.sub(r'\s{2,}', ' ', body_text).strip()
    
    return abstract_text, body_text

def generate_record(xml_string):
    abstract_text, body_text = extract_abstract_and_body_text(xml_string)
    return f"{abstract_text} {body_text}".strip()
