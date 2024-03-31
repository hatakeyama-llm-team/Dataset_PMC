from concurrent.futures import ProcessPoolExecutor, as_completed
import glob
import spacy
from text_extraction import extract_abstract_and_body_text
from tqdm import tqdm

# TODO: 精度向上のため、ロードするモデルの変更を検討する
# TODO: 軽量化のため、不要なパイプラインコンポーネントの非活性化を検討する
nlp = spacy.load("en_core_sci_lg-0.5.4/en_core_sci_lg/en_core_sci_lg-0.5.4", disable=[])
nlp.add_pipe("sentencizer")

def is_complete_sentence(sentence):
    # 空の文、または非常に短い文を排除
    if not sentence or len(sentence) < 3:
        return False
    # 文末の記号で基本的な完全性をチェック
    if sentence[-1] not in [".", "?", "!"]:
        return False
    # 単語数でフィルタリング
    if len(sentence.split()) < 3:
        return False
    return True

def count_text_sentences(text, return_sentences=False, filter_incomplete=True):
    doc = nlp(text)
    sentences = [sentence.text.strip() for sentence in doc.sents if is_complete_sentence(sentence.text.strip())] if filter_incomplete else [sentence.text.strip() for sentence in doc.sents]

    if return_sentences:
        return sentences
    else:
        return len(sentences)


def analyze_text_sentences(file_path):
    abstract_text, body_text = extract_abstract_and_body_text(file_path)
    sentences_data = []
    # TODO: 重複の削除を行う

    # abstractから文を抽出し、リストに追加
    abstract_sentences = count_text_sentences(abstract_text, return_sentences=True)
    for sentence in abstract_sentences:
        sentences_data.append({"File_Path": file_path, "Sentence": sentence, "Target": "abstract"})

    # bodyから文を抽出し、リストに追加
    body_sentences = count_text_sentences(body_text, return_sentences=True)
    for sentence in body_sentences:
        sentences_data.append({"File_Path": file_path, "Sentence": sentence, "Target": "body"})

    return sentences_data

def process_files_in_parallel(directory, file_selection):
    all_xml_files = sorted(glob.glob(f"{directory}/**/*.xml", recursive=True))

    if file_selection == 'first100':
        selected_xml_files = all_xml_files[:100]
    elif file_selection == 'last100':
        selected_xml_files = all_xml_files[-100:]
    elif file_selection == 'all':
        selected_xml_files = all_xml_files
    else:
        print("Invalid file selection option. Using 'all' as default.")
        selected_xml_files = all_xml_files

    sentences_data = []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_text_sentences, file_path) for file_path in selected_xml_files]
        for future in tqdm(as_completed(futures), total=len(selected_xml_files), desc="Processing Files"):
            sentences_data.extend(future.result())
    return sentences_data