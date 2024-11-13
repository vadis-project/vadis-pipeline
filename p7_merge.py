# %%
from tqdm import tqdm
from pathlib import Path
from vadis_logger import vadis_logger
from helper import save_json, load_json
import numpy as np
from config import config
import json
import re
import spacy
import requests

# %%
process = 'Result Merge'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = Path('vadis_app_ssoar_list.json')
f_metadata = Path('metadata.json')
# %%
f_varident_sm_order = Path('data/svident_sm_ord.json')
f_varident_3_xrlfen_zeros4 = Path('data/svident_auto.json')
f_d_summaries = Path('data/summaries_.json')
# %%
grobid_config_path = config['grobid']['config_path']
grobid_process_type = config['grobid']['process_type']

dir_pdf_raw = config['corpus_paths']['pdf_raw']
dir_json_raw = config['corpus_paths']['json_raw']
dir_json_text = config['corpus_paths']['json_text']
dir_filtered_json_text = config['corpus_paths']['filtered_json_text']

MIN_SEN_LENTH = 35

with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

# %%
varident_sm_order = load_json(f_varident_sm_order)
varident_3_xrlfen_zeros4 = load_json(f_varident_3_xrlfen_zeros4)
d_summaries = load_json(f_d_summaries)
# %%
l_valid_pub_ids = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and v['parsed_json_raw']]

with open('data/varident_4_xrlfen_zeros4.json', 'r') as f:
    d_svident_auto = json.load(f)
with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

l_valid_pub_ids_2 = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and k not in d_svident_auto.keys() and v['parsed_json_raw']]
l_valid_pub_ids_3 = [k for k, v in d_summaries.items()]

# %%    STATPRINT

# %%
nlp_en = spacy.load('en_core_web_sm')
nlp_de = spacy.load('de_core_news_sm')

# %% PREPROC


def contains_url(sentence: str) -> bool:
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.search(regex, sentence)
    return url is not None


def too_long(sentence: str, n: int = 500) -> bool:
    return len(sentence) > n


def too_short(sentence: str, n: int = 45) -> bool:
    return len(sentence) < n


def too_many_nr_chars(sentence: str, n: int = 40) -> bool:

    nr_chars = sum(c.isdigit() for c in sentence)
    return nr_chars > n


def too_many_nonalnum_chars(sentence: str, n: int = 20) -> bool:
    nr_chars = len(re.findall(r'[^a-zA-Z0-9 ]', sentence))
    return nr_chars > n


def table_ref(sentence: str) -> bool:
    if 'Table' in sentence:
        return True
    return False


def validate_sentence(sentence: str, p_too_long: int, p_too_short: int) -> bool:
    '''Detects sentences that do not meet multiple requirements like:
    - not containing an url
    - not being too longs
    - not containing too many number characters
    - not referring to tables

    Args:
      sentence: A document sentence as string.

    Returns:
      A Boolean of whether the sentence meets the requirements listed above .
    '''
    if contains_url(sentence):
        print('contains_url', sentence)
        return False
    elif too_long(sentence, p_too_long):
        print('too_long', sentence)
        return False
    elif too_short(sentence, p_too_short):
        print('too_short', sentence)
        return False
    elif too_many_nr_chars(sentence):
        print('too_many_nr_chars', sentence)
        return False
    elif too_many_nonalnum_chars(sentence):
        print('too_many_nonalnum_chars', sentence)
        return False
    '''elif table_ref(sentence):
        print('table_ref')
        return False'''
    return True


def word_count(text: str) -> int:
    return


def generate_hash(_input):

    return str(abs(hash(_input)) % (10 ** 10))


def variable_id_separator(l_ids: str):

    ids = l_ids.split(',')
    return (str(ids[0]), (ids[1:]))


def variable_id_mod(item: dict, limit=5):
    '''
    Args:
        item: variables listed explicitly
        limit: number of variables to keep, the rest will be shrinked and their id's will be shown
    Returns:
        new_item: only limited number of variables listed explicitly, the others only have ids
    '''

    new_item = {}
    similar_variables = []
    var_cnt = 0
    for k, v in item.items():
        new_k = k
        if 'exploredata' in k:
            var_cnt += 1

            if var_cnt <= limit:
                ids = k.split(',')
                new_k = str(ids[0])
                if len(ids) > 1:
                    similar_variables += ids[1:]
                new_item[new_k] = v
            else:
                similar_variables += k.split(',')
        else:
            new_item[new_k] = v
    new_item['similar_variables'] = similar_variables
    return new_item


# %%
url_gsq = config['urls']['gesis_search_query']
d_id_bs = {}
d_id_bss = {}
l_abs = []
for id in l_valid_pub_ids_3[:]:
    try:
        print(id)
        response = requests.post(url_gsq + id, timeout=15)

        abstract = response.json()['hits']['hits'][0]['_source']['abstract']
        # fulltext = response.json()['hits']['hits'][0]['_source']['fulltext']
        l_abs.append(abstract)
        d_id_bs[id] = abstract

        nlp = nlp_en
        if response.json()['hits']['hits'][0]['_source']['coreLanguage'] == 'de':
            nlp = nlp_de

        string1 = abstract
        doc = nlp(string1)

        l_abssen = []
        order = 0
        for sent in doc.sents:
            order += 1
            abssen = {'sentence': str(sent), 'position': order, 'sentence_id': generate_hash(abstract)}
            l_abssen.append(abssen)

        d_id_bss[id] = l_abssen
        print(order)

    except KeyError as ke:
        vadis_logger.error(ke)
        # raise
        print('fail')

# %%



# %%
#%% OLD VERSION
#VADIS KG OLD VERSION

mode_write = False
mode_update = True
d_doc_varsencnt = {}
cnt_var = 0
cnt_nvar_doc = 0
cnt_var_sm = 0
cnt_var_auto = 0

l_doc_final = []
for ssoar_id in (l_valid_pub_ids[:]):
    cnt_pub_sent = 0

    if True:
        print(f'reading {ssoar_id}')
        
        d_pub_vadisdata = {}

        try:
            d_pub_vadisdata['abs_summary'] = d_summaries[ssoar_id]['gen_sum']
            d_pub_vadisdata['abs_summary_id'] = generate_hash(d_summaries[ssoar_id]['gen_sum'])
            d_pub_vadisdata['ext_summary'] = d_summaries[ssoar_id]['ext_sum']
            #d_pub_vadisdata['ext_summary_id'] = generate_hash(d_summaries[ssoar_id]['ext_sum'])
            d_pub_vadisdata['abstract_sentences'] = d_id_bss[ssoar_id]
            l_doc_final.append(ssoar_id)
        except:
            continue

        d_vs_matc = varident_sm.get(ssoar_id, {})
        d_vs_auto = varident_zeros4.get(ssoar_id, {})

        # new combined dict
        d_sv_update = {}


        l_vs_auto_lim_s = [k for k in d_vs_auto.keys() if  validate_sentence(k, 360, 35)]
        l_vs_sm_lim_s = [v['sentence'] for v in d_vs_matc.values() if  validate_sentence(v['sentence'], 360, 35)]
        #print(f'Total fw matchings: {len(l_vs_sm_lim_s)} Total auto matchings: {len(l_vs_auto_lim_s)}. Min Sentence Lenth = {MIN_SEN_LENTH}')

        for k, v in d_vs_matc.items():
            if v['sentence'] in d_vs_auto.keys():
                #print('MATCH',v['sentence'])
                pass

        l_vs_auto_sm_diff = list(set(l_vs_auto_lim_s).difference(set(l_vs_sm_lim_s)))
        l_vs_sm_auto_diff = list(set(l_vs_sm_lim_s).difference(set(l_vs_auto_lim_s)))
        l_vs_auto_sm_intsec = list(set(l_vs_auto_lim_s).intersection(set(l_vs_sm_lim_s)))
        l_vs_auto_sm_union = list(set(l_vs_auto_lim_s).union(set(l_vs_sm_lim_s)))
        print(len(l_vs_auto_lim_s), len(l_vs_sm_lim_s), len(l_vs_auto_sm_union))
        #d_sv_update
        for i_sv, sv in enumerate(l_vs_auto_sm_union):
            # CASE 1:
            # CASE 2: common ones
            d_temp_meta = {}
            d_temp_meta['id'] = generate_hash(sv + str(cnt_pub_sent))
            cnt_pub_sent+=1
            
            d_temp_meta['score'] = 0 #d_vs_auto[sv]['score']
            d_temp_meta['common_words'] = []

            d_item = {}
            #print(sv)
            l_type = []

            for d_item_v in d_vs_matc.values():
                if d_item_v['sentence'] == sv:
                    l_type.append('SM')
                    #print(1,sv)
                    #print(2,d_item_v['var_text'])
                    d_item = d_item_v

                    if len(sv) > len(d_item_v['var_text']):
                        d_temp_meta[d_item_v['var_ids'][0]] = d_item_v['var_text'] # + ' ~score: ' + str(d_item_v['score']/100)
                        d_temp_meta['score'] = str(max(float(d_temp_meta['score']), (d_item_v['score']/100)))
                        break

            if sv in d_vs_auto.keys():
                for k, v in d_vs_auto[sv].items():
                    l_type.append('ZEROS4')
                    if 'exploredata' in k:
                        d_temp_meta[k] = v
                        d_temp_meta['common_words'] = d_vs_auto[sv]['common_words']
                        d_temp_meta['score'] = str(max(float(d_temp_meta['score']), float(d_vs_auto[sv]['score'])))



            d_temp_meta['type'] = ''.join(list(set(l_type)))

            if float(d_temp_meta['score']) > 0.5:
                d_temp_meta = variable_id_mod(d_temp_meta)
                d_sv_update[sv] = d_temp_meta



        d_sv_update_sorted = sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True)
        d_sv_update_sorted = dict(sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True))


        print(len(d_sv_update_sorted))
        d_doc_varsencnt[ssoar_id] = len(d_sv_update_sorted)
        if mode_write:
            output_dir = Path('pubs_var_sentences_vars_v5')
            output_file = output_dir / Path(ssoar_id + '.json')

            with open(output_file, mode='w') as out_f:
                json.dump(d_sv_update_sorted, out_f)
            print(output_file)



        ld_varsen_data = []
        for k, v in d_sv_update_sorted.items():
            d_varsen_data = {}
            d_varsen_data['sentence'] = k
            d_varsen_data['score'] = v['score']
            d_varsen_data['id'] = v['id']
            d_varsen_data['type'] = v['type']
            d_varsen_data['similar_variables'] = v['similar_variables']
            if 'common_words' in v.keys():
                d_varsen_data['common_words'] = v['common_words']
            else:
                d_varsen_data['common_words'] = []

            l_varids = [key for key, val in v.items() if 'exploredata-' in key]
            l_vartexts = [val for key, val in v.items() if 'exploredata-' in key]
            l_vars = list(zip(l_varids, l_vartexts))
            ld_vardata = []
            for (a, b) in l_vars:
                d_var_data = {}
                d_var_data['var_id'] = a
                d_var_data['research_dataset_id'] = a.replace('exploredata-', '').split('_')[0] #"exploredata-ZA3205_Varv90"
                d_var_data['var_text'] = b
                ld_vardata.append(d_var_data)
            
            d_varsen_data['variables'] = ld_vardata
            ld_varsen_data.append(d_varsen_data)
        d_pub_vadisdata['variable_sentences'] = ld_varsen_data
        cnt_var += len(ld_varsen_data)
        if len(ld_varsen_data)==0:
            cnt_nvar_doc += 1
        if mode_update:
            output_dir = Path('pubs_var_sentences_vars_v6')
            output_file = output_dir / Path(ssoar_id + '.json')

            with open(output_file, mode='w') as out_f:
                json.dump(d_pub_vadisdata, out_f)
            print(output_file)

#%% 



# %% OLD VERSION
# VADIS ORIGINAL 3
mode_write = True
mode_update = True
thrh_scr = 0.0
d_doc_varsencnt = {}
cnt_var = 0
cnt_nvar_doc = 0
cnt_var_sm = 0
cnt_var_auto = 0
output_dir = Path('vadis_data')
l_doc_final = []
for ssoar_id in (l_valid_pub_ids_3[:]):
    cnt_pub_sent = 0

    if ssoar_id not in l_valid_pub_ids_2:
        print(f'reading {ssoar_id}')

        d_pub_vadisdata = {}

        try:
            d_pub_vadisdata['abstractive_summary'] = d_summaries[ssoar_id]['gen_sum']
            d_pub_vadisdata['abstractive_summary_id'] = generate_hash('[gen_sum]' + d_summaries[ssoar_id]['gen_sum'])
            d_pub_vadisdata['extractive_summary'] = d_summaries[ssoar_id]['ext_sum']
            d_pub_vadisdata['extractive_summary_id'] = generate_hash('[ext_sum]' + d_summaries[ssoar_id]['ext_sum'])
            d_pub_vadisdata['abstract_sentences'] = d_id_bss[ssoar_id]
            l_doc_final.append(ssoar_id)

            d_vs_matc = varident_sm_order.get(ssoar_id, {})
            d_vs_auto = varident_3_xrlfen_zeros4.get(ssoar_id, {})

            # new combined dict
            d_sv_update = {}

            # l_vs_auto_lim_s = [k for k in d_vs_auto.keys() if  validate_sentence(k, 360, 35)]
            l_vs_auto_lim_s = [k for k, v in d_vs_auto.items() if validate_sentence(v['sentence'], 600, 35)]
            l_vs_sm_lim_s = [k for k, v in d_vs_matc.items() if validate_sentence(v['sentence'], 600, 35)]
            print(l_vs_auto_lim_s)
            # print(f'Total fw matchings: {len(l_vs_sm_lim_s)} Total auto matchings: {len(l_vs_auto_lim_s)}. Min Sentence Lenth = {MIN_SEN_LENTH}')

            for k, v in d_vs_matc.items():
                if v['sentence'] in d_vs_auto.keys():
                    # print('MATCH',v['sentence'])
                    pass

            l_vs_auto_sm_diff = list(set(l_vs_auto_lim_s).difference(set(l_vs_sm_lim_s)))
            l_vs_sm_auto_diff = list(set(l_vs_sm_lim_s).difference(set(l_vs_auto_lim_s)))
            l_vs_auto_sm_intsec = list(set(l_vs_auto_lim_s).intersection(set(l_vs_sm_lim_s)))
            l_vs_auto_sm_union = list(set(l_vs_auto_lim_s).union(set(l_vs_sm_lim_s)))
            print(len(l_vs_auto_lim_s), len(l_vs_sm_lim_s), len(l_vs_auto_sm_union))
            #d_sv_update
            for i_sv, sv in enumerate(l_vs_auto_sm_union):
                # CASE 1:
                # CASE 2: common ones
                d_temp_meta = {}
                d_temp_meta['order'] = sv
                d_temp_meta['id'] = generate_hash(ssoar_id + sv)
                cnt_pub_sent += 1

                d_temp_meta['score'] = 0  # d_vs_auto[sv]['score']
                d_temp_meta['common_words'] = []

                d_item = {}
                # print(sv)
                l_type = []

                # for d_item_k, d_item_v in d_vs_matc.items():
                if sv in d_vs_matc.keys():
                    d_item_v = d_vs_matc[sv].copy()
                    l_type.append('SM')
                    # print(1,sv)
                    # print(2,d_item_v['var_text'])
                    d_item = d_item_v

                    # if d_item_v['var_list'] is a list
                    d_var_list = {}
                    d_var_list = d_var_list | d_item_v['var_list']
                    '''for var in d_item_v['var_list']:
                        d_var_list = d_var_list | var'''

                    if len(d_var_list) > 0:
                        l_scores = []
                        for var_k, var_v in d_var_list.items():
                            if len(d_item_v['sentence']) > len(var_v['var_text']):
                                d_temp_meta[var_k] = var_v['var_text'] + ' ~similarity score: ' + str(int(var_v['var_sim_score']))
                                #d_temp_meta['score'] = str(max(float(d_temp_meta['score']), (d_item_v['score']/100)))
                                l_scores.append(var_v['var_sim_score'])
                                break
                        d_temp_meta['score'] = str((max(d_temp_meta['score'], np.average(l_scores)/100)))

                '''for d_item_v in d_vs_matc.values():
                    if d_item_v['sentence'] == sv:
                        l_type.append('SM')
                        #print(1,sv)
                        #print(2,d_item_v['var_text'])
                        d_item = d_item_v

                        if len(sv) > len(d_item_v['var_text']):
                            d_temp_meta[d_item_v['var_ids'][0]] = d_item_v['var_text'] # + ' ~score: ' + str(d_item_v['score']/100)
                            d_temp_meta['score'] = str(max(float(d_temp_meta['score']), (d_item_v['score']/100)))
                            break'''

                if sv in d_vs_auto.keys():
                    d_item_v = d_vs_auto[sv].copy()
                    #print(d_item_v)
                    # if d_item_v['var_list'] is a list
                    d_var_list = {}
                    for var in d_item_v['var_list']:
                        d_var_list = d_var_list | var
                    print(d_var_list)
                    if len(d_var_list) > 0:
                        #print(sv)
                        l_type.append('AUTO')
                        d_temp_meta['common_words'] = d_vs_auto[sv]['common_words']
                        d_temp_meta['score'] = str((max(float(d_item_v['score'])/100, float(d_temp_meta['score']))))
                        for d_var_k, d_var_v in d_var_list.items():
                            #print(d_var_k, d_var_v['var_text'] )
                            #v['var_text'] = v['var_text'] + ' ~score: ' + str(int(v['var_sim_score']))
                            d_temp_meta[d_var_k] = d_var_v['var_text'] + ' ~similarity score: ' + str(int(d_var_v['var_sim_score']))
                '''if sv in d_vs_auto.keys():
                    for k, v in d_vs_auto[sv].items():
                        l_type.append('ZEROS4')
                        if 'exploredata' in k:
                            d_temp_meta[k] = v
                            d_temp_meta['common_words'] = d_vs_auto[sv]['common_words']
                            d_temp_meta['score'] = str(max(float(d_temp_meta['score']), float(d_vs_auto[sv]['score'])))'''

                # print(d_item_v['sentence'],d_temp_meta['score'])

                d_temp_meta['methodType'] = ''.join(list(set(l_type)))

                if float(d_temp_meta['score']) > thrh_scr:
                    d_temp_meta = variable_id_mod(d_temp_meta)
                    # d_sv_update[sv] = d_temp_meta
                    d_sv_update[d_item_v['sentence']] = d_temp_meta
        except KeyError as kr:
            # raise
            print('K_ERR', kr)
            continue

        d_sv_update_sorted = sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True)
        d_sv_update_sorted = dict(sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True))

        print(len(d_sv_update_sorted))
        d_doc_varsencnt[ssoar_id] = len(d_sv_update_sorted)

        ld_varsen_data = []

        cnt_sim = 0
        for k, v in d_sv_update_sorted.items():
            d_varsen_data = {}
            d_varsen_data['sentence'] = k
            d_varsen_data['score'] = v['score']
            d_varsen_data['id'] = v['id']
            d_varsen_data['methodType'] = v['methodType']
            d_varsen_data['similar_variables'] = v['similar_variables']
            if 'common_words' in v.keys():
                d_varsen_data['common_words'] = v['common_words']
            else:
                d_varsen_data['common_words'] = []

            l_varids = [key for key, val in v.items() if 'exploredata-' in key]
            l_vartexts = [val for key, val in v.items() if 'exploredata-' in key]
            l_vars = list(zip(l_varids, l_vartexts))
            ld_vardata = []
            for (a, b) in l_vars:
                d_var_data = {}
                d_var_data['var_id'] = a
                d_var_data['research_dataset_id'] = a.replace('exploredata-', '').split('_')[0] #"exploredata-ZA3205_Varv90"
                d_var_data['var_text'] = b
                ld_vardata.append(d_var_data)
            
            d_varsen_data['variables'] = ld_vardata
            ld_varsen_data.append(d_varsen_data)
            cnt_sim += len(v['similar_variables'])
        d_pub_vadisdata['variable_sentences'] = ld_varsen_data
        cnt_var += len(ld_varsen_data) #+ cnt_sim
        if len(ld_varsen_data)==0:
            cnt_nvar_doc += 1
        if mode_update:
            #output_dir = Path('pubs_var_sentences_vars_v6')
            output_file = output_dir / Path(ssoar_id + '.json')

            with open(output_file, mode='w') as out_f:
                json.dump(d_pub_vadisdata, out_f)
            print(output_file)

# %%
import os
ssoar_files = [Path(f).stem for f in os.listdir('vadis_data') if f.endswith('.json')]
d_1 = {}
for sf in ssoar_files:
    file_data = load_json('vadis_data/' + sf + '.json')
    d_1[sf] = len(file_data['variable_sentences'])
l_1 = ssoar_files

#%% OLD VERSION
#VADIS ORIGINAL

mode_write = False
mode_update = False
thrh_scr = 0.0
d_doc_varsencnt = {}
cnt_var = 0
cnt_nvar_doc = 0
cnt_var_sm = 0
cnt_var_auto = 0
output_dir = Path('vadis_data')
l_doc_final = []
for ssoar_id in (l_valid_pub_ids[:]):
    cnt_pub_sent = 0

    if True:
        print(f'reading {ssoar_id}')
        
        d_pub_vadisdata = {}

        try:
            d_pub_vadisdata['abs_summary'] = d_summaries[ssoar_id]['gen_sum']
            d_pub_vadisdata['ext_summary'] = d_summaries[ssoar_id]['ext_sum']
            l_doc_final.append(ssoar_id)
        except:
            continue

        d_vs_matc = varident_sm.get(ssoar_id, {})
        d_vs_auto = varident_3_xrlfen_zeros4.get(ssoar_id, {})

        # new combined dict
        d_sv_update = {}


        l_vs_auto_lim_s = [k for k in d_vs_auto.keys() if  validate_sentence(k, 360, 35)]
        l_vs_sm_lim_s = [v['sentence'] for v in d_vs_matc.values() if  validate_sentence(v['sentence'], 360, 35)]
        #print(f'Total fw matchings: {len(l_vs_sm_lim_s)} Total auto matchings: {len(l_vs_auto_lim_s)}. Min Sentence Lenth = {MIN_SEN_LENTH}')

        for k, v in d_vs_matc.items():
            if v['sentence'] in d_vs_auto.keys():
                #print('MATCH',v['sentence'])
                pass

        l_vs_auto_sm_diff = list(set(l_vs_auto_lim_s).difference(set(l_vs_sm_lim_s)))
        l_vs_sm_auto_diff = list(set(l_vs_sm_lim_s).difference(set(l_vs_auto_lim_s)))
        l_vs_auto_sm_intsec = list(set(l_vs_auto_lim_s).intersection(set(l_vs_sm_lim_s)))
        l_vs_auto_sm_union = list(set(l_vs_auto_lim_s).union(set(l_vs_sm_lim_s)))
        print(len(l_vs_auto_lim_s), len(l_vs_sm_lim_s), len(l_vs_auto_sm_union))
        #d_sv_update
        for i_sv, sv in enumerate(l_vs_auto_sm_union):
            # CASE 1:
            # CASE 2: common ones
            d_temp_meta = {}
            d_temp_meta['id'] = generate_hash(sv + str(cnt_pub_sent))
            cnt_pub_sent+=1
            
            d_temp_meta['score'] = 0 #d_vs_auto[sv]['score']
            d_temp_meta['common_words'] = []

            d_item = {}
            #print(sv)
            l_type = []

            for d_item_v in d_vs_matc.values():
                if d_item_v['sentence'] == sv:
                    l_type.append('SM')
                    #print(1,sv)
                    #print(2,d_item_v['var_text'])
                    d_item = d_item_v

                    if len(sv) > len(d_item_v['var_text']):
                        d_temp_meta[d_item_v['var_ids'][0]] = d_item_v['var_text'] # + ' ~score: ' + str(d_item_v['score']/100)
                        d_temp_meta['score'] = str(max(float(d_temp_meta['score']), (d_item_v['score']/100)))
                        break

            if sv in d_vs_auto.keys():
                for k, v in d_vs_auto[sv].items():
                    l_type.append('ZEROS4')
                    if 'exploredata' in k:
                        d_temp_meta[k] = v
                        d_temp_meta['common_words'] = d_vs_auto[sv]['common_words']
                        d_temp_meta['score'] = str(max(float(d_temp_meta['score']), float(d_vs_auto[sv]['score'])))



            d_temp_meta['type'] = ''.join(list(set(l_type)))

            if float(d_temp_meta['score']) > thrh_scr:
                d_temp_meta = variable_id_mod(d_temp_meta)
                d_sv_update[sv] = d_temp_meta



        d_sv_update_sorted = sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True)
        d_sv_update_sorted = dict(sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True))


        print(len(d_sv_update_sorted))
        d_doc_varsencnt[ssoar_id] = len(d_sv_update_sorted)



        ld_varsen_data = []

        cnt_sim = 0
        for k, v in d_sv_update_sorted.items():
            d_varsen_data = {}
            d_varsen_data['sentence'] = k
            d_varsen_data['score'] = v['score']
            d_varsen_data['id'] = v['id']
            d_varsen_data['type'] = v['type']
            d_varsen_data['similar_variables'] = v['similar_variables']
            if 'common_words' in v.keys():
                d_varsen_data['common_words'] = v['common_words']
            else:
                d_varsen_data['common_words'] = []

            l_varids = [key for key, val in v.items() if 'exploredata-' in key]
            l_vartexts = [val for key, val in v.items() if 'exploredata-' in key]
            l_vars = list(zip(l_varids, l_vartexts))
            ld_vardata = []
            for (a, b) in l_vars:
                d_var_data = {}
                d_var_data['var_id'] = a
                d_var_data['research_dataset_id'] = a.replace('exploredata-', '').split('_')[0] #"exploredata-ZA3205_Varv90"
                d_var_data['var_text'] = b
                ld_vardata.append(d_var_data)
            
            d_varsen_data['variables'] = ld_vardata
            ld_varsen_data.append(d_varsen_data)
            cnt_sim += len(v['similar_variables'])
        d_pub_vadisdata['variable_sentences'] = ld_varsen_data
        cnt_var += len(ld_varsen_data) #+ cnt_sim
        if len(ld_varsen_data)==0:
            cnt_nvar_doc += 1
        if mode_update:
            #output_dir = Path('pubs_var_sentences_vars_v6')
            output_file = output_dir / Path(ssoar_id + '.json')

            with open(output_file, mode='w') as out_f:
                json.dump(d_pub_vadisdata, out_f)
            print(output_file)
# %%
import requests 

d_id_bs = {}
l_abs = []
# for id in l_doc_final:
for id in ssoar_files:
    url3 = "http://searchtest.gesis.org/searchengine?q=_id:" + id
    doc = json.loads(requests.get(url3).content)
    try:
        dt = doc['hits']['hits'][0]['_source']['date']
        d_id_bs[id] = dt
    except:
        print(id, 'fail')

# %%
d_doc_varsencnt = d_1.copy()
d_id_bs_sorted = dict(sorted(d_id_bs.items(), key=lambda item: item[1], reverse=True))
d_doc_varsencnt_sorted = dict(sorted(d_doc_varsencnt.items(), key=lambda item: item[1], reverse=True))
'''with open('vadis_pub_list_year.txt', 'w') as f:
    for key in d_id_bs_sorted.keys():
        f.write(key)
        f.write(',\n')
with open('vadis_pub_list_varsencnt.txt', 'w') as f:
    for key in d_doc_varsencnt_sorted.keys():
        f.write(key)
        f.write(',\n')'''

d_doc_list = {}
d_doc_list['random_ids'] = ssoar_files # l_doc_final
d_doc_list['vs_count_desc_ids'] = list(d_doc_varsencnt_sorted.keys())
d_doc_list['year_desc_ids'] = list(d_id_bs_sorted.keys())

# %%
save_json(d_doc_list, 'data/vadis_app_ssoar_list.json')
# %%
import pickle as pkl
with open('dict_ssoar_sum_.pkl', 'rb') as f:
    d_abssum = pkl.load(f)



# %%

d_asum = {}
for k, v in d_summaries.items():
    d_asum[k.replace('gesis-ssoar-','')] = v[gen_sum]

# %%

# %%

# %%

# %%

# %%




# %%

for ssoar_id in tqdm(l_valid_pub_ids[:1]):
    cnt_pub_sent = 0

    if True:
        print(f'reading {ssoar_id}')
        


        d_vs_matc = varident_sm.get(ssoar_id, {})
        d_vs_auto = varident_zeros4 .get(ssoar_id, {})

        # new combined dict
        d_sv_update = {}

        # CASE 1: if there are only AUTO results
        if not d_vs_matc:
            
            print(f'{ssoar_id} has NOT string matchings')

            for k, v in d_vs_auto.items():

                print(type(k), k)
                print(v)
                if not filter_out_doc_sentences(k, 360, 35):
                    print(f'filter out: {k}')

                if validate_sentence(k):
                    d_temp_meta = d_vs_auto[k]
                    
                    temp_score = (float(d_temp_meta['score']))
                    d_temp_meta['id'] = generate_hash(k + str(cnt_pub_sent))
                    cnt_pub_sent+=1
                    d_temp_meta['type'] = '[AUTO]'

                    d_temp_meta = variable_id_mod(d_temp_meta)

                    if temp_score > 0.5:
                        d_sv_update[k] = d_temp_meta
                    


            #d_sv_update = {'[AUTO] ' + k:v for k, v in d_vs_auto.items()}

        else:
            print(f'{ssoar_id} has string fw matchings')

            #print(d_vs_matc)

            l_vs_auto_lim_s = [k for k in d_vs_auto.keys() if validate_sentence(k)]
            l_vs_sm_lim_s = [v['sentence'] for v in d_vs_matc.values() if validate_sentence(v['sentence'])]
            #print(f'Total fw matchings: {len(l_vs_sm_lim_s)} Total auto matchings: {len(l_vs_auto_lim_s)}. Min Sentence Lenth = {MIN_SEN_LENTH}')

            for k, v in d_vs_matc.items():
                if v['sentence'] in d_vs_auto.keys():
                    #print('MATCH',v['sentence'])
                    pass

            l_vs_auto_sm_diff = list(set(l_vs_auto_lim_s).difference(set(l_vs_sm_lim_s)))
            l_vs_sm_auto_diff = list(set(l_vs_sm_lim_s).difference(set(l_vs_auto_lim_s)))
            l_vs_auto_sm_intsec = list(set(l_vs_auto_lim_s).intersection(set(l_vs_sm_lim_s)))
            
            #print(len(l_vs_auto_sm_diff), len(l_vs_sm_auto_diff), len(l_vs_auto_sm_intsec))


            #d_sv_update
            for i_sv, sv in enumerate(l_vs_auto_lim_s):
                d_temp_meta = {}
                # CASE 1:
                if sv in l_vs_auto_sm_diff:
                    d_temp_meta = d_vs_auto[sv]

                    temp_score = (float(d_temp_meta['score']))
                    
                    
                    d_temp_meta['type'] = '[AUTO]'
                    d_temp_meta['id'] = generate_hash(sv + str(cnt_pub_sent))
                    cnt_pub_sent+=1
                    
                    d_temp_meta = variable_id_mod(d_temp_meta)

                    if temp_score > 0.5:
                        d_sv_update[sv] = d_temp_meta #d_vs_auto[sv]
                
                # CASE 2: common ones
                elif sv in l_vs_auto_sm_intsec:
                    d_temp_meta = {}
                    d_temp_meta['id'] = generate_hash(sv + str(cnt_pub_sent))
                    cnt_pub_sent+=1
                    
                    d_temp_meta['score'] = d_vs_auto[sv]['score']

                    d_item = {}
                    #print(sv)
                    for d_item_v in d_vs_matc.values():
                        if d_item_v['sentence'] == sv:
                            #print(1,sv)
                            #print(2,d_item_v['var_text'])
                            d_item = d_item_v

                            if len(sv) > len(d_item_v['var_text']):
                                d_temp_meta[d_item_v['var_ids'][0]] = '[SM] ' + d_item_v['var_text'] # + ' ~score: ' + str(d_item_v['score']/100)
                    #print(d_item_v)

                    for k, v in d_vs_auto[sv].items():
                        if 'exploredata' in k:
                            d_temp_meta[k] = '[AUTO] ' + v

                    d_temp_meta['common_words'] = d_vs_auto[sv]['common_words']
                    d_temp_meta['type'] = '[AUTO][SM]'

                    d_temp_meta = variable_id_mod(d_temp_meta)

                    d_sv_update[sv] = d_temp_meta

            # CASE 3:
            for sv in l_vs_sm_auto_diff:
                d_temp_meta = {}
                    

                d_item = {}
                for d_item_v in d_vs_matc.values():
                    if d_item_v['sentence'] == sv:
                        d_item = d_item_v

                        if len(sv) > len(d_item_v['var_text']):

                            d_temp_meta[d_item_v['var_ids'][0]] = '[SM] ' + d_item_v['var_text'] # + ' ~score: ' + str(d_item_v['score']/100)
                            d_temp_meta['score'] = str(d_item_v['score']/100)
                            d_temp_meta['id'] = generate_hash(sv + str(cnt_pub_sent))
                            cnt_pub_sent+=1
                            d_temp_meta['type'] = 'SM'

                            d_temp_meta = variable_id_mod(d_temp_meta)

                            d_sv_update[sv] = d_temp_meta



                #print(d_sv_update)

        d_sv_update_sorted = sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True)
        d_sv_update_sorted = dict(sorted(d_sv_update.items(), key=lambda item: item[1]['score'], reverse=True))


        print(len(d_sv_update_sorted))
        #d_doc_varsencnt['"gesis-ssoar-' + f.name.replace('_variables.json', '') + '"'] = len(d_sv_update_sorted)
        if mode_write:
            output_dir = Path('pubs_var_sentences_vars_v5')
            output_file = output_dir / Path(ssoar_id + '.json')

            with open(output_file, mode='w') as out_f:
                json.dump(d_sv_update_sorted, out_f)
            print(output_file)


        d_pub_vadisdata = {}
        d_pub_vadisdata['abs_summary'] = d_summaries[ssoar_id]['gen_sum']
        d_pub_vadisdata['ext_summary'] = d_summaries[ssoar_id]['ext_sum']
        ld_varsen_data = []
        for k, v in d_sv_update_sorted.items():
            d_varsen_data = {}
            d_varsen_data['sentence'] = k
            d_varsen_data['score'] = v['score']
            d_varsen_data['id'] = v['id']
            d_varsen_data['type'] = v['type']
            d_varsen_data['similar_variables'] = v['similar_variables']
            if 'common_words' in v.keys():
                d_varsen_data['common_words'] = v['common_words']
            else:
                d_varsen_data['common_words'] = []

            l_varids = [key for key, val in v.items() if 'exploredata-' in key]
            l_vartexts = [val for key, val in v.items() if 'exploredata-' in key]
            l_vars = list(zip(l_varids, l_vartexts))
            ld_vardata = []
            for (a, b) in l_vars:
                d_var_data = {}
                d_var_data['var_id'] = a
                d_var_data['var_text'] = b
                ld_vardata.append(d_var_data)
            
            d_varsen_data['variables'] = ld_vardata
            ld_varsen_data.append(d_varsen_data)
        d_pub_vadisdata['variable_sentences'] = ld_varsen_data
        if mode_update:
            output_dir = Path('pubs_var_sentences_vars_v6')
            output_file = output_dir / f.name

            with open(output_file, mode='w') as out_f:
                json.dump(d_pub_vadisdata, out_f)
            print(output_file)


# %%

# %%

vadis_logger.info(f'PROCESS FINISHED: {process}.')