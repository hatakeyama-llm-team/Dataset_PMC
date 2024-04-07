from concurrent.futures import ProcessPoolExecutor, as_completed
import glob
import spacy
from text_extraction import extract_abstract_and_body_text
from tqdm import tqdm

# TODO: 精度向上のため、ロードするモデルの変更を検討する
# TODO: 軽量化のため、不要なパイプラインコンポーネントの非活性化を検討する
nlp = spacy.load("en_core_sci_lg-0.5.4/en_core_sci_lg/en_core_sci_lg-0.5.4", disable=[])
nlp.add_pipe("sentencizer")

def count_text_sentences(text):
    doc = nlp(text)
    return len(list(doc.sents))

def analyze_text_sentences(file_path):
    abstract_text, body_text = extract_abstract_and_body_text(file_path)
    abstract_sentences_count = count_text_sentences(abstract_text)
    body_sentences_count = count_text_sentences(body_text)
    
    # Updated to save as a single record
    sentences_data = {
        "File_Path": file_path,
        "Abstract": abstract_text,
        "Abstract_Sentences_Count": abstract_sentences_count,
        "Body": body_text,
        "Body_Sentences_Count": body_sentences_count
    }
    return sentences_data

def process_files_in_parallel(directory, file_selection):
    all_xml_files = sorted(glob.glob(f"{directory}/**/*.xml", recursive=True))

    if file_selection == 'first10':
        selected_xml_files = all_xml_files[:10]
    elif file_selection == 'last10':
        selected_xml_files = all_xml_files[-10:]
    else:
        selected_xml_files = all_xml_files

    sentences_data = []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(analyze_text_sentences, file_path) for file_path in selected_xml_files]
        for future in tqdm(as_completed(futures), total=len(selected_xml_files), desc="Processing Files"):
            sentences_data.append(future.result())
    return sentences_data
