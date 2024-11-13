# %%
import orjson
from tqdm import tqdm
from pathlib import Path
import json

from helper import save_json
from vadis_logger import vadis_logger
from config import config

# %%

process = 'proceed to sentences files'
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
# %%
output_dir = Path(dir_filtered_json_text)
output_dir.mkdir(parents=True, exist_ok=True)

# %%
files = [f for f in Path(dir_json_text).iterdir()]
# %%

for f in tqdm(files[:]):
    output_file = output_dir / f.name

    with open(f, 'r', encoding='utf8') as f_i:
        paper_json = orjson.loads(f_i.read())
        len(paper_json)
        all_sections = set([v['section'] for v in paper_json.values() if 'section' in v.keys()])
        print(all_sections)
        d_section_sentences = {}
        for section in all_sections:
            sentences = [sentence for v in paper_json.values() if ('section' in v.keys() and v['section']==section) for sentence in v['sentences'] ]
            d_section_sentences[section] = sentences

        d_section_sentences['FIG'] = [sentence for v in paper_json.values() if ('section' not in v.keys()) for sentence in v['sentences'] ]

        all_sentences = [s for v in d_section_sentences.values() for s in v]
        print(f' {f.name}: {len(all_sentences)}')

        d_doc = {}
        d_doc['sentences'] = all_sentences
        with open(output_file, 'wb') as f_o:
            f_o.write(orjson.dumps(d_section_sentences))

# %%
vadis_logger.info(f'PROCESS FINISHED: {process}.')
# %%
