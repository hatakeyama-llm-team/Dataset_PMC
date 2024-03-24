import xml.etree.ElementTree as ET

def extract_abstract_and_body_text(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        abstract_text = "".join(root.find('.//front/article-meta/abstract').itertext()) if root.find('.//front/article-meta/abstract') is not None else ""
        body_text = "".join(root.find('.//body').itertext()) if root.find('.//body') is not None else ""
        return abstract_text, body_text
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return "", ""
