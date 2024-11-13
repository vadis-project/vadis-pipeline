
# %%
from doc2json.grobid2json import tei_to_json
from grobid_client.grobid_client import GrobidClient
import json
import os
from pathlib import Path
from tqdm.notebook import tqdm
from lingua import Language, LanguageDetectorBuilder
import pysbd
import orjson
import time

# %%
from helper import save_json
from vadis_logger import vadis_logger
from config import config


# %%
process = 'parse PDF files'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = 'vadis_app_ssoar_list.json'
f_metadata = 'metadata.json'

grobid_config_path = config['grobid']['config_path']
grobid_process_type = config['grobid']['process_type']

dir_pdf_raw = config['corpus_paths']['pdf_raw']
dir_json_raw = config['corpus_paths']['json_raw']

with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

# %%


def grobid_process_files_pdf2xml(grobid_client: GrobidClient, process_type: str, p_input_dir: str):
    grobid_client.process(process_type, p_input_dir, n=20)


def grobid_process_files_xml2json(p_input_dir: str, p_output_dir: str):

    assert os.path.exists(p_input_dir)

    files = [os.path.join(p_input_dir, f) for f in os.listdir(p_input_dir) if f.endswith('.xml')]

    if not os.path.exists(p_output_dir):
        os.makedirs(p_output_dir)

    failed_files = []

    for f in tqdm(files):
        output_path = os.path.join(p_output_dir, os.path.basename(f).split('.')[0]+'.json')
        if os.path.exists(output_path):
            continue

        try:
            paper = tei_to_json.convert_tei_xml_file_to_s2orc_json(f)
        except Exception as err:
            vadis_logger.error(err)
            failed_files.append(f)
            continue

        res_json = paper.release_json()

        with open(output_path, 'w', encoding='utf8') as fp:
            json.dump(res_json, fp)

    vadis_logger.info(f'Succeeded/All: {len(files)-len(failed_files)}/{len(files)}')


# %%
grobid_client = GrobidClient(config_path=grobid_config_path)
# %%
grobid_process_files_pdf2xml(grobid_client, grobid_process_type, dir_pdf_raw)
# %%
grobid_process_files_xml2json(dir_pdf_raw, dir_json_raw)

# %%
ssoar_parsed_files = ['gesis-ssoar-' + Path(f).stem for f in os.listdir(dir_json_raw) if f.endswith('.json')]

# %%
l_all_pub = d_metadata.keys()
for id in l_all_pub:
    if id in ssoar_parsed_files:
        d_metadata[id]['parsed_json_raw'] = True
    else:
        
        print(id)
        d_metadata[id]['parsed_json_raw'] = False
# %%
l_all_pub = d_metadata.keys()
for id in l_all_pub:
    if id in ssoar_parsed_files:
        d_metadata[id]['parsed_json_raw'] = True
    else:
        d_metadata[id]['parsed_json_raw'] = False
# %%
save_json(d_metadata, 'metadata.json')
vadis_logger.info(f'PROCESS FINISHED: {process}.')
# %%
process = 'parse JSON files into PARAG'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = Path('vadis_app_ssoar_list.json')
f_metadata = Path('metadata.json')

grobid_config_path = config['grobid']['config_path']
grobid_process_type = config['grobid']['process_type']

dir_pdf_raw = config['corpus_paths']['pdf_raw']
dir_json_raw = config['corpus_paths']['json_raw']
dir_json_text = config['corpus_paths']['json_text']


# %%
with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

# %%


def clean_text(text):
    text = text.replace('¬ ', '')
    text = text.replace('  ', ' ')
    text = text.replace('\\', '')
    text = text.replace('-</td></tr><tr><td>', '')  # PDF grobid-specific line-breaks
    text = text.replace('</td></tr><tr><td>', ' ')  # inject space after line-breaks
    return text


def get_pysbd_lang(language):
    if language == Language.ENGLISH:
        return 'en'
    elif language == Language.FRENCH:
        return 'fr'
    elif language == Language.GERMAN:
        return 'de'
    elif language == Language.ITALIAN:
        return 'it'
    elif language == Language.RUSSIAN:
        return 'ru'
    elif language == Language.SPANISH:
        return 'es'
    else:
        return ''


def get_segmenter(text):
    languages = [Language.ENGLISH, Language.GERMAN, Language.ITALIAN, Language.RUSSIAN, Language.SPANISH, Language.FRENCH]
    detector = LanguageDetectorBuilder.from_languages(*languages).build()
    pysbd_segmenters = {'en': pysbd.Segmenter(language='en', clean=True), 'fr': pysbd.Segmenter(language='fr', clean=True), 'de': pysbd.Segmenter(language='de', clean=True), 'it': pysbd.Segmenter(language='it', clean=True), 'ru': pysbd.Segmenter(language='ru', clean=True), 'es': pysbd.Segmenter(language='es', clean=True)}

    language = detector.detect_language_of(text)
    lang = get_pysbd_lang(language)
    if lang == '':
        return '', lang
    else:
        return pysbd_segmenters[lang], lang


def split_into_paragraphs(paper_json):
    paragraphs = {}

    for i, p in enumerate(paper_json['pdf_parse']['body_text']):
        i = str(i)
        text = p['text']
        text = clean_text(text)
        if text != '':
            segmenter, lang = get_segmenter(text)
            if segmenter == '':
                # TODO: apply cleaning (segmenter also has a cleaner)
                paragraphs[i] = {'sentences': [text], 'lang': lang, 'section': p['section']}
            else:
                paragraphs[i] = {'sentences': segmenter.segment(text), 'lang': lang, 'section': p['section']}

    for j, (_, p) in enumerate(paper_json['pdf_parse']['ref_entries'].items()):  # TODO: how to fit sentences (including wrongly recognized ones) from figures into the right context?
        j = 'FIG'+str(len(paper_json['pdf_parse']['body_text'])+j)
        # print(p.keys())

        for key in ['content', 'text']:
            if key in p.keys():
                # for text in [p['content'], p['text']]:
                text = p[key]
                text = clean_text(text)
                if text != '':
                    segmenter, lang = get_segmenter(text)
                    if segmenter == '':
                        paragraphs[j] = {'sentences': [text], 'lang': lang}
                    else:
                        paragraphs[j] = {'sentences': segmenter.segment(text), 'lang': lang}

    return paragraphs


def parse(input_dir, output_dir, overwrite, last_modified_days):
    # print('Started.')
    files = [f for f in Path(input_dir).iterdir()]
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    skipped_files = 0
    failed_files = 0
    created_files = 0

    for f in tqdm(files):
        output_file = output_dir / f.name

        if not overwrite:  # skip if the file already exists
            if output_file.is_file():
                skipped_files += 1
                continue

        if last_modified_days:  # skip if the file has been modified within the past N days
            if output_file.is_file():
                if (time.time() - (86400*last_modified_days)) < output_file.stat().st_mtime:
                    skipped_files += 1
                    continue

        try:
            with open(f, 'rb') as f:
                paper_json = orjson.loads(f.read())
        except Exception as err:
            vadis_logger.error(err)
            print(f'Unable to read file: {f}')  # skip if the file cannot be opened
            failed_files += 1
            continue

        try:
            paragraphs = split_into_paragraphs(paper_json)
            with open(output_file, 'wb') as f:
                f.write(orjson.dumps(paragraphs))
            created_files += 1
        except Exception as err:
            vadis_logger.error(err)
            print(f'Failed generating paragraphs for file: {f}')  # skip if the file cannot be processed
            failed_files += 1

    print(f'Skipped files: {skipped_files}')
    print(f'Failed files: {failed_files}')
    print(f'Created files: {created_files}')
    print('Finished.')


# %%
parse(input_dir=dir_json_raw, output_dir=dir_json_text, overwrite=True, last_modified_days=0)
# %%
vadis_logger.info(f'PROCESS FINISHED: {process}.')
