# %%
import langdetect
import json
import requests
from pathlib import Path
import os
import re
import spacy

from helper import check_json, save_json
from vadis_logger import vadis_logger
from config import config
# %%
process = 'retrieve research datasets of documents'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_doc_types = config['doc_types']
l_lang = config['languages']
url_gsq = config['urls']['gesis_search_query']

b_dataset_validity = True
b_download_variable = False

d_lang_query = dict()
for lang in l_lang:
    d_query = dict()
    d_query['body'] = config['queries']['gws_ssoar_withds'][lang]['body']
    d_query['result'] = config['queries']['gws_ssoar_withds'][lang]['result']
    d_lang_query[lang] = d_query

# %%


def fetch_vars(l_vars):
    dict_varid_var = dict()
    for var_id in l_vars:
        url_req = url_gsq + var_id
        content = json.loads(requests.get(url_req, timeout=20).content)

        cnt_hits = len(content['hits']['hits'])

        if cnt_hits > 0:
            hit = content['hits']['hits'][0]['_source']
            dict_varid_var[var_id] = hit
    return dict_varid_var


def path_vocab(rd):
    return Path('vocab/' + rd + '.json')


'''def save_json(f_name, d):
    with open(f_name + '.json', 'w', encoding='utf8') as fp:
        json.dump(d, fp)


def save_path_json(f_name, d):
    with open(f_name, 'w', encoding='utf8') as fp:
        json.dump(d, fp)'''


# %%
d_metadata = {}
f_pub_list = 'vadis_app_ssoar_list.json'
d_id = {'ssoar_ids': []}
save_json(d_id, f_pub_list)
try:
    for lang in l_lang:

        l_all_rrd = []
        if b_dataset_validity:
            f_pubs = d_lang_query[lang]['result']

            vadis_logger.debug(f'reading {f_pubs}')
            with open(f_pubs, encoding='utf8') as f:
                j_pubs = json.load(f)

            abst_same_lang = False
            l_pubs_ids = []
            s_rd_ids = set()
            d_pub_rrd = {}

            vadis_logger.info(f'{len(j_pubs["hits"])}: number of docs')
            for pub in j_pubs['hits']:
                d_pub_data = {}
                d_pub_data['lang'] = lang
                l_rrd = []

                l_b = []
                b_type = False      # is doc type in predefined doc types
                b_abst = False      # are abstract and doc lang the same
                b_rrd = False       # this is guaranteed to be TRUE by initial document list POST request

                source = pub['_source']

                if 'document_type' in source.keys():
                    if source['document_type'] in l_doc_types:
                        b_type = True
                    else:
                        vadis_logger.debug(f'{source["id"]} has invalid type: {source["document_type"]}')
                l_b.append(b_type)

                if abst_same_lang and 'abstract' in source.keys():
                    if langdetect.detect(source['abstract']) == lang:
                        b_abst = True
                    l_b.append(b_abst)

                if 'related_research_data' in source.keys():
                    b_rrd = True
                    l_rrd = [rdd['id'] for rdd in source['related_research_data']]
                l_b.append(b_rrd)

                if all(l_b):
                    l_pubs_ids.append(source['id'])
                    d_pub_rrd[source['id']] = [rdd['id'] for rdd in source['related_research_data']]
                    # s_rd_ids = s_rd_ids.union(set([rdd['id'] for rdd in source['related_research_data']]))

                d_pub_data['all_related_research_datasets_list'] = l_rrd
                l_all_rrd.extend(l_rrd)
                d_metadata[source['id']] = d_pub_data

            save_json(d_pub_rrd, 'd_pub_rrd_'+lang)
            vadis_logger.info(f'{len(l_pubs_ids)}: selected type publications with res ds, abstract same lang ({abst_same_lang})')

            d_rd_isvalid = {}
            d_rd_lvar = {}
            d_rd_isdown = {}

            s_rd_ids = set(l_all_rrd)
            for rd in s_rd_ids:
                url_req = url_gsq + rd
                content = json.loads(requests.get(url_req).content)
                vars = []
                d_rd_isvalid[rd] = False
                # print(len(content['hits']['hits']))
                if len(content['hits']['hits']) > 0:
                    hit = content['hits']['hits'][0]
                    stated_var_cnt = 0 
                    actual_var_cnt = 0
                    if 'number_variables' in hit['_source'].keys():
                        try:
                            stated_var_cnt = int(hit['_source']['number_variables'])
                        except Exception as err:
                            vadis_logger.error(err)
                            vadis_logger.debug(f'{rd} has invalid number_variables')
                    if 'related_variables' in hit['_source'].keys():
                        vars = [ele['id'] for ele in hit['_source']['related_variables']]
                        # d_rd_lvar[rd] = vars
                        actual_var_cnt = len(vars)

                    if actual_var_cnt-stated_var_cnt >= 0 and actual_var_cnt > 0:
                        d_rd_isvalid[rd] = True
                        d_rd_lvar[rd] = vars
                        vadis_logger.debug(f'{rd} has all variables of {actual_var_cnt} - VALID ')

                        '''if b_download_variable:
                            d_var = fetch_vars(vars)
                            check_json('vocab/' + rd + '.json', d_var)
                            d_rd_isdown[rd] = True'''

                    else:
                        vadis_logger.debug(f'{rd} has NOT all variables of {actual_var_cnt} - INVALID')

                    vadis_logger.debug(f'{actual_var_cnt} variables found for {rd}; {stated_var_cnt} stated ')

            vadis_logger.info(f'{list(d_rd_isvalid.values()).count(True)} of {len(d_rd_isvalid)} RDs are valid.')
            vadis_logger.info(f'{len(d_rd_lvar)} RD-variable lists available')
            save_json(d_rd_isvalid, 'd_rd_isvalid_' + lang)
            save_json(d_rd_lvar, 'd_rd_lvar_'+lang)
except Exception as err:
    raise
    vadis_logger.error(err)

# %%
try:
    l_id = []
    for lang in l_lang:
        #   LOAD DICTS
        with open('d_rd_isvalid_'+lang+'.json', 'r') as f:
            d_rd_isvalid = json.load(f)
        with open('d_rd_lvar_'+lang+'.json', 'r') as f:
            d_rd_lvar = json.load(f)
        with open('d_pub_rrd_'+lang+'.json', 'r') as f:
            d_pub_rrd = json.load(f)

        d_pub_vadis = {}
        d_pub_availablerrd = {}
        for k, v in d_pub_rrd.items():

            if any([d_rd_isvalid[rd] for rd in v]):
                # d_pub_vadis[k] = any([d_rd_isvalid[rd] for rd in v])
                l_id.append(k)
                d_pub_availablerrd[k] = [rd for rd in v if d_rd_isvalid[rd]]
                d_metadata[k]['available_related_research_datasets_list'] = [rd for rd in v if d_rd_isvalid[rd]]
                d_metadata[k]['vadis_valid'] = True
            else:
                d_metadata[k]['available_related_research_datasets_list'] = []
                d_metadata[k]['vadis_valid'] = False

        vadis_logger.info(f'{len(d_pub_availablerrd)} {lang} documents will be processed for VADIS')
        save_json(d_pub_vadis, 'd_pub_vadis_'+lang)
        save_json(d_pub_availablerrd, 'd_pub_availablerrd_'+lang)

        if b_download_variable:
            vadis_logger.info('STARTED: downloading variables')
            s_rrd = set([rd for l_rd in d_pub_rrd.values() for rd in l_rd])
            for rd, vars in d_rd_lvar.items():
                if d_rd_isvalid[rd] and rd in s_rrd:
                    if not Path('vocab/' + rd + '.json').exists():
                        d_var = fetch_vars(vars)
                        check_json('vocab/' + rd + '.json', d_var)
                        d_rd_isdown[rd] = True
                    else:
                        vadis_logger.debug(f'{rd} already exists.')

            vadis_logger.info('FINISHED: downloading variables')

    d_id['ssoar_ids'] = l_id
    save_json(d_id, f_pub_list)
    save_json(d_metadata, 'metadata')

except Exception as err:
    raise
    vadis_logger.error(err)

# %%
# %%    VOCAB MERGE rd_filtering
nlp_en = spacy.load('en_core_web_sm')
nlp_de = spacy.load('de_core_news_sm')
l_lang = ['en', 'de']
d_rd_quescnt = {}
# %%

# folder path
src_dir_path = r'vocab/'
dest_dir_path = r'sampled_vocab/'

# list file and directories
res = os.listdir(src_dir_path)

for idx, f in enumerate(res[:]):

    in_file = src_dir_path + f
    out_file = dest_dir_path + f

    vadis_logger.info(f'{in_file} ({idx}/{len(res)}) started.')

    vocab = {}

    try:
        with open(in_file, 'r', encoding='utf8') as f:
            vocab = json.load(f)
    except Exception as err:
        vadis_logger.error(err)
        pass

    d_voc_all = {}
    d_lang_quescnt = {}
    for lang in l_lang:
        vadis_logger.info(f'{lang}')
        nlp = nlp_de
        KW_QT = 'question_text'  # German
        KW_AC = 'answer_categories'
        KW_VL = 'variable_label'
        KW_T = 'topic'

        if lang == 'en':
            nlp = nlp_en
            KW_QT = 'question_text_en'
            KW_AC = 'answer_categories_en'
            KW_VL = 'variable_label_en'
            KW_T = 'topic_en'

        l_qu = []
        l_top = []
        l_lbl = []
        l_qutop = []
        l_toplbl = []
        l_qulbl = []

        d_voc = {}

        print(len(vocab.items()))
        for k, v in vocab.items():
            cr = []
            t_qu_en = ''
            t_ans = ''
            t_top = ''
            t_lbl = ''
            l_qu_ner = []
            l_ans_ner = []

            if KW_QT in v.keys():
                local_qu_en = v[KW_QT]

                t_qu_en = (local_qu_en).replace('<br/>', '')

                ents = nlp(t_qu_en).ents

                for e in ents:
                    if len(re.findall('[<>0-9]', e.text)) == 0 and len(e.text) > 3:
                        l_qu_ner.append(e.text)

            if KW_AC in v.keys():
                t_ans = (v[KW_AC]).replace('<br/>', '')
                ents = nlp_en(t_ans).ents

                for e in ents:
                    if len(re.findall('[<>0-9]', e.text)) == 0 and len(e.text) > 3:
                        l_ans_ner.append(e.text)

            if KW_VL in v.keys():
                t_lbl = v[KW_VL]
                cr.append(v[KW_VL])
            if KW_T in v.keys():
                t_top = ' '.join(v[KW_T])
                cr += (v[KW_T])

            d_var = d_voc.get(t_qu_en, {})

            l_qu.append(t_qu_en)
            l_qulbl.append(t_qu_en + t_lbl)
            l_qutop.append(t_qu_en + '~' + t_top)
            l_lbl.append(t_lbl)
            l_toplbl.append(t_top + t_lbl)
            l_top.append(t_top)

            # d_var['id'] = v['id']
            d_var['question_lang'] = v.get('question_lang', '')
            d_var['question_text'] = t_qu_en
            d_var['answer_categories'] = t_ans
            d_var['topic'] = t_top
            d_var['question_text_ner'] = l_qu_ner
            d_var['answer_categories_ner'] = l_ans_ner

            l_labels_en = d_var.get('labels', [])
            l_labels_en = (l_labels_en) + [(t_lbl)]
            d_var['labels'] = l_labels_en

            l_ids = d_var.get('var_ids', [])
            l_ids = (l_ids) + [v['id']]
            d_var['var_ids'] = l_ids

            d_voc[t_qu_en] = d_var

        '''with open(out_file, 'w', encoding='utf8') as f:
            json.dump(d_voc, f)'''
        d_voc_all[lang] = d_voc

        vadis_logger.info(f'{len(l_qu)}: all questions, {len(set(l_qu))}: uniq questions')
        vadis_logger.info(f'{len(l_lbl)}: all labels, {len(set(l_lbl))}: uniq labels')
        vadis_logger.info(f'{len(l_top)}: all topics, {len(set(l_top))}: uniq labels')

        d_lang_quescnt[lang] = len(set(l_qu))

    d_rd_quescnt[f.name] = d_lang_quescnt
    save_json(d_voc_all, (out_file))
    vadis_logger.info(f'{out_file} is saved')

# %%
save_json(d_rd_quescnt, 'd_rd_quescnt.json')

# %%
process = 'retrieve research datasets of documents'
vadis_logger.info(f'PROCESS FINISHED: {process}.')

# %%
