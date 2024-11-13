# %%
from pathlib import Path
from typing import List
from vadis_logger import vadis_logger
from helper import save_json
from config import config
import json

import requests
from pydantic import BaseModel
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from schnitsum import SchnitSum
from summarizer.sbert import SBertSummarizer

import langdetect
from transformers.pipelines import pipeline
# %%
process = 'Summarization'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = Path('vadis_app_ssoar_list.json')
f_metadata = Path('metadata.json')

grobid_config_path = config['grobid']['config_path']
grobid_process_type = config['grobid']['process_type']

dir_pdf_raw = config['corpus_paths']['pdf_raw']
dir_json_raw = config['corpus_paths']['json_raw']
dir_json_text = config['corpus_paths']['json_text']
dir_filtered_json_text = config['corpus_paths']['filtered_json_text']

with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)
    
with open('d_summaries_all.json', 'r') as f:
    d_pre = json.load(f)

# %%


class Texts(BaseModel):
    documents: List[str]


lang2model_name = {
    "en": "sobamchan/bart-large-scitldr",
    "de": "sobamchan/mbart-large-xscitldr-de",
}

lang2lang_code = {
    "en": "en_XX",
    "de": "de_DE",
    "ja": "ja_XX",
    "it": "it_IT",
    "zh": "zh_CN",
}


class Model:
    def __init__(self, tgt_lang: str = "en", use_gpu=False):
        print(f"Initializing a model in {tgt_lang}...")
        # model_name = lang2model_name[tgt_lang]

        model_name = lang2model_name[tgt_lang]
        self.schnitsum_model = SchnitSum(model_name, tgt_lang=tgt_lang, use_gpu=use_gpu)

        self.use_gpu = use_gpu
        self.tgt_lang = tgt_lang
        self.model_name = model_name

        self.translator = None
        print("loaded!")

    def summarize(self, text: str) -> str:
        do_translation = True if langdetect.detect(text) == "de" else False
        if do_translation:
            # print("German!!! Translate to English first.")
            if self.translator is None:
                self.translator = pipeline(model="facebook/wmt19-de-en")
            text = self.translator([text])[0]["translation_text"]
            # print(text)

        return self.schnitsum_model([text])[0]

    def summarize_batch(self, texts: List[str]) -> List[str]:
        return self.schnitsum_model(texts)


# %%
model_en = Model(tgt_lang='en')
model_de = Model(tgt_lang='de')
# %%
ext_model = SBertSummarizer("all-distilroberta-v1")

# %%

# %%    TRY LATER
lang = 'en'
print(f"Initializing a model in {lang}...")
model_name_en = lang2model_name[lang]
tokenizer_en = AutoTokenizer.from_pretrained(model_name_en, use_auth_token=True)
bart_en = AutoModelForSeq2SeqLM.from_pretrained(model_name_en, use_auth_token=True)

lang = 'de'
print(f"Initializing a model in {lang}...")
model_name_de = lang2model_name[lang]
tokenizer_de = AutoTokenizer.from_pretrained(model_name_de, use_auth_token=True)
bart_de = AutoModelForSeq2SeqLM.from_pretrained(model_name_de, use_auth_token=True)

# %%
'''def summarize_batch(tokenizer, bart, texts: List[str]) -> List[str]:
    inputs = tokenizer(
        texts, padding="max_length", truncation=True, return_tensors="pt"
    )
    summary_ids = bart.generate(
        inputs["input_ids"],
        max_length=50,
        num_beams=1,
        early_stopping=True,
    )
    return tokenizer.batch_decode(summary_ids, skip_special_tokens=True)'''

# %%
l_valid_pub_ids = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and v['parsed_json_raw']]
l_valid_pub_ids = [k for k, v in d_metadata.items() if 'all_related_research_datasets_list' in v.keys() and len(v['all_related_research_datasets_list'])>0 and 'ssoar' in k and k not in d_pre.keys()]

# %%
url_gsq = config['urls']['gesis_search_query']
d_id_bs = {}
l_abs = []
for id in l_valid_pub_ids[:]:
    try:
        print(id)
        response = requests.post(url_gsq + id, timeout=15)

        abstract = response.json()['hits']['hits'][0]['_source']['abstract']
        # fulltext = response.json()['hits']['hits'][0]['_source']['fulltext']
        l_abs.append(abstract)
        d_id_bs[id] = abstract
    except Exception as err:
        vadis_logger.error(err)
        print('fail')

# %%

d_id_sum = {}

model = model_en
for id, abs in d_id_bs.items():

    print(d_metadata[id]['lang'])
    if d_metadata[id]['lang'] == 'de':
        model = model_de
    else:
        model = model_en
    sum = {}
    sum['gen_sum'] = model.summarize(abs)
    sum['ext_sum'] = ext_model(abs, num_sentences=1)
    d_id_sum[id] = sum
    print(id)
# %%
save_json(d_id_sum, 'data/summaries_.json')

# %%
