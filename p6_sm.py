# %%
from fuzzywuzzy import fuzz
import json
import orjson
from pathlib import Path
import orjson
from vadis_logger import vadis_logger
from config import config
# %%
process = 'String Matching'
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


l_valid_pub_ids = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and v['parsed_json_raw']]

# %% here must be same as in p6 auto file
d_paper_idx_sent = {}
for idx, ssoar_id in enumerate(l_valid_pub_ids[:]):
    id = ssoar_id.replace('gesis-ssoar-', '')
    with open(dir_filtered_json_text + '/' + id + '.json', 'rb') as f_i:
        paper_json = orjson.loads(f_i.read())


    idxcnt_sent = 0
    d_idx_sent = {}
    for section, sentences in paper_json.items():

        ssentences = [s[:512] for s in sentences]

        z_sp = ssentences
        for sent in z_sp:
            d_idx_sent[idxcnt_sent] = {'sentence': sent}
            idxcnt_sent+=1
    d_paper_idx_sent[ssoar_id] = d_idx_sent


# %%

d_doc_var_sent_fw = {}
d_doc_var_sent_fw_ord = {}
for ssoar_id in l_valid_pub_ids_2[:]:

    id = ssoar_id.replace('gesis-ssoar-', '')
    print(l_valid_pub_ids_2.index(ssoar_id))

    if ssoar_id not in d_doc_var_sent_fw_ord:
        try:

            related_research_datasets = d_metadata[ssoar_id]['available_related_research_datasets_list']


            col_id = []
            col_sentence = []
            col_is_variable = []
            col_lang = []
            col_variable = []
            col_uuid = []

            sentence_tokens = []

            d_var_sent = {}
            d_var_sent_ord = {}

            '''paper_json = {}
            with open(dir_filtered_json_text + '/' + id + '.json', 'rb') as f_i:
                paper_json = orjson.loads(f_i.read())'''
                #sentence_tokens = paper_json['sentences']
            lang = d_metadata[ssoar_id]['lang']
            d_match_ord = {}
            var_list = []
            for vocab in related_research_datasets:
                # print(vocab)
                with open('corpus/sampled_vocab/' + vocab + '.json', 'r', encoding='utf8') as f:
                    loaded_dict = json.load(f)
                    #print(loaded_dict)

                    for k in loaded_dict[lang].keys():
                        '''for section, sentences in paper_json.items():
                            #for s in sentence_tokens:
                            for s in sentences:
                                if len(s)>=35 and len(k)>=35:
                                    #print(s, k)
                                    ratio = fuzz.partial_ratio(k, s)
                                    if ratio>= 75:
                                        d_match = {'score': ratio, 'var_text': k, 'sentence': s, 'sentence_order': sentences.index(s), 'rd_id': vocab, 'var_ids': loaded_dict[lang][k]['var_ids'], 'section': section}
                                        print(d_match)
                                        d_var_sent[k] = d_match'''

                        d_idx_sent = d_paper_idx_sent[ssoar_id]
                        for ord, sent in d_idx_sent.items():
                            s = sent['sentence']
                            
                            d_match_sent = d_match_ord.get(ord, {})
                            var_list = d_match_ord.get('var_list', {})
                            if len(s)>=35 and len(k)>=35:
                                #print(s, k)
                                ratio = fuzz.partial_ratio(k, s)
                                if ratio>= 75:
                                    d_match_var = {loaded_dict[lang][k]['var_ids'][0]: {'var_sim_score': ratio, 'var_text': k, 'rd_id': vocab, 'similar_var_list': loaded_dict[lang][k]['var_ids'][1:]}}
                                    #print(d_match)
                                    var_list = var_list | (d_match_var)
                            if len(var_list)>0:

                                d_match_sent['sentence'] = s
                                d_match_sent['var_list'] = var_list
                                d_match_ord[ord] = d_match_sent
                            if len(var_list)>1:
                                print('here')
                                print(d_match_sent)
            '''for vocab in related_research_datasets:
                print(vocab)
                with open('corpus/sampled_vocab/' + vocab + '.json', 'r', encoding='utf8') as f:
                    loaded_dict = json.load(f)
                    #print(loaded_dict)

                    for k in loaded_dict[lang].keys():

                        d_idx_sent = d_paper_idx_sent[ssoar_id]
                        for ord, sent in d_idx_sent.items():
                            s = sent['sentence']
                            
                            d_match_sent = {}
                            if len(s)>=35 and len(k)>=35:
                                #print(s, k)
                                ratio = fuzz.partial_ratio(k, s)
                                if ratio>= 75:
                                    d_match_var = {loaded_dict[lang][k]['var_ids'][0]: {'var_sim_score': ratio, 'var_text': k, 'rd_id': vocab, 'similar_var_list': loaded_dict[lang][k]['var_ids'][1:]}}
                                    #print(d_match)
                                    var_list.append(d_match_var)
                            if len(var_list)>0:

                                d_match_sent['sentence'] = s
                                d_match_sent['var_list'] = var_list
                                d_match_ord[ord] = d_match_sent'''
            d_doc_var_sent_fw_ord[ssoar_id] = d_match_ord

            d_doc_var_sent_fw[ssoar_id] = d_var_sent

        except Exception as err:
            # raise
            vadis_logger.error(ssoar_id, err)
            print(ssoar_id, err)

# %%
with open('data/svident_sm_ord.json', 'w') as outfile:
    json.dump(d_doc_var_sent_fw_ord, outfile)
# %%

# %%
vadis_logger.info(f'PROCESS FINISHED: {process}.')
# %%
