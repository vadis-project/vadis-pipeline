# %%
import json
import pandas as pd
import uuid

from pathlib import Path

from vadis_logger import vadis_logger
from config import config
# %%

# %%
process = 'Pre AUTO SV-Ident'
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

# %%


def load_path_json(f_name: Path):
    d = {}
    if f_name.exists():
        with open(f_name, 'r', encoding='utf8') as fp:
            d = json.load(fp)
    return d


def save_path_json(f_name: Path, d):
    with open(f_name, 'w', encoding='utf8') as fp:
        json.dump(d, fp)


d_metadata = load_path_json(f_metadata)

# %%
df_en = pd.read_csv('corpus/svident_gt/en.tsv', sep='\t')
df_en_neg = df_en[df_en['is_variable'] == 0]
df_train = pd.read_csv('corpus/svident_gt/train.tsv', sep='\t')
df_de = df_train[df_train['lang'] == 'de']
df_de_neg = df_de[df_de['is_variable'] == 0]

# %%
dict_prop_corpus = {}
d_doc_var_sent_fw = {}
for lang in l_lang:
    l_valid_pub_ids = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and v['parsed_json_raw'] and v['lang'] == lang]
    print(len(l_valid_pub_ids))
    for ssoar_id in l_valid_pub_ids[:]:
        try:
            print(ssoar_id)
            lang = d_metadata[ssoar_id]['lang']
            related_research_datasets = d_metadata[ssoar_id]['available_related_research_datasets_list']
            col_id = []
            col_sentence = []
            col_is_variable = []
            col_lang = []
            col_variable = []
            col_uuid = []

            for vocab in related_research_datasets:
                # vocab = related_research_datasets[0]
                print(vocab)
                with open('corpus/sampled_vocab/' + vocab + '.json', 'r', encoding='utf8') as f:
                    loaded_dict = json.load(f)

                for k, v in loaded_dict[lang].items():

                    # print(k)
                    col_id.append(','.join(v['var_ids']))
                    t_qu_ansner = str(v['question_text']) + ' ' + ' '.join(v['answer_categories_ner'])
                    # print(t_qu_ansner)
                    # col_sentence.append(k)
                    col_sentence.append(t_qu_ansner)
                    col_is_variable.append(1)
                    col_lang.append('en')
                    col_variable.append(','.join(v['var_ids']))
                    col_uuid.append(str(uuid.uuid4()))
            var_dict = {'id': col_id, 'sentence': col_sentence, 'is_variable': col_is_variable, 'lang': col_lang, 'variable': col_variable, 'uuid': col_uuid} 

            var_df = pd.DataFrame(var_dict)

            if 'en' == lang:

                non_var_df = df_en_neg[:len(var_df)] if len(var_df) >= 8 else df_en_neg[:8]
                join_df = pd.concat([var_df, non_var_df], ignore_index=True)
                print(f'var_df: {len(var_df)}, non_var_df: {len(non_var_df)}, join_df: {len(join_df)}')

                join_df.to_csv('corpus/svident_ner/' + ssoar_id + '.tsv', sep='\t')
                dict_prop_corpus[ssoar_id] = True
            elif 'de' == lang:
                var_df.to_csv('corpus/svident_ner/' + ssoar_id + '.tsv', sep='\t')
                non_var_df = df_de_neg[:len(var_df)] if len(var_df) >= 8 else df_de_neg[:8]
                join_df = pd.concat([var_df, non_var_df], ignore_index=True)
                print(f'var_df: {len(var_df)}, non_var_df: {len(non_var_df)}, join_df: {len(join_df)}')

                join_df.to_csv('corpus/svident_ner/' + ssoar_id + '.tsv', sep='\t')
                dict_prop_corpus[ssoar_id] = True

        except Exception as err:
            vadis_logger.error(err)
            dict_prop_corpus[ssoar_id] = False
            # raise
            print(f'error: {ssoar_id}')

# %%

vadis_logger.info(f'PROCESS FINISHED: {process}.')