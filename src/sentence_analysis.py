import spacy
from text_extraction import extract_abstract_and_body_text

# TODO: 精度向上のため、ロードするモデルの変更を検討する
# TODO: 軽量化のため、不要なパイプラインコンポーネントの非活性化を検討する
nlp = spacy.load("en_core_sci_lg-0.5.4/en_core_sci_lg/en_core_sci_lg-0.5.4", disable=[])
nlp.add_pipe("sentencizer")

def count_text_sentences(text):
    doc = nlp(text)
    return len(list(doc.sents))

def analyze_text_sentences(file_path):
    abstract_text, body_text = extract_abstract_and_body_text(file_path)
    combined_text = f"{abstract_text}\n\n{body_text}"
    return combined_text
