# %%
import torch
import numpy as np
from typing import List
from pydantic import BaseModel
import pandas as pd

from config import config, d_svi_settings
from helper import get_label
from index import load_embeddings, make_embeddings_name, make_index
from model import load_model
from nltk.corpus import stopwords
import pickle

import orjson
from pathlib import Path
from vadis_logger import vadis_logger
import json
from helper import save_json
from transformers import pipeline
from faiss import IndexIVFFlat
from scipy.special import logit, expit
# %%

process = 'SVIdent - Auto'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = Path('vadis_app_ssoar_list.json')
f_metadata = Path('metadata.json')
f_svident_auto = Path('data/varident_zeros4.json')

grobid_config_path = config['grobid']['config_path']
grobid_process_type = config['grobid']['process_type']

dir_pdf_raw = config['corpus_paths']['pdf_raw']
dir_json_raw = config['corpus_paths']['json_raw']
dir_json_text = config['corpus_paths']['json_text']
dir_filtered_json_text = config['corpus_paths']['filtered_json_text']

files = [f for f in Path(dir_json_text).iterdir()]

with open(f_svident_auto, 'r') as f:
    d_svident_auto = json.load(f)
with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

l_valid_pub_ids = [k for k, v in d_metadata.items() if 'parsed_json_raw' in v.keys() and k not in d_svident_auto.keys() and v['parsed_json_raw']]
# %%


class Query(BaseModel):
    text: str
    order: int


class QueryList(BaseModel):
    queries: List[Query]


def search_batch(query: QueryList, search_index: IndexIVFFlat, top_k: int = 10, lang: str = 'en', score_method: str = 'sdcg'):
    query_embs = model.encode([q.text for q in query.queries])
    norm = np.linalg.norm(query_embs, axis=1)  # normalize vectors to unit length, inner product (dot product) is equal to cosine similarity
    query_embs = np.asarray([query_embs[i]/norm[i] for i in range(query_embs.shape[0])])
    distances, corpus_ids = search_index.search(query_embs, top_k)

    results = []
    for q, d, cid in zip(query.queries, distances, corpus_ids):
        hits = [{'corpus_id': id, 'score': score} for id, score in zip(cid, d)]
        hits = sorted(hits, key=lambda x: x['score'], reverse=True)
        texts = [(embeddings_dict['sentences'][idx], embeddings_dict['ids'][idx], scr, int(embeddings_dict['labels'][idx])) for (idx, scr) in zip(cid, d)]
        label, pos_score, neg_score = get_label(hits[0:top_k], embeddings_dict['labels'], method=score_method, return_scores=True)

        results.append({'query': q, 'pred_label': label, 'pred_pos_score': pos_score, 'pred_neg_score': neg_score, 'top_k_texts': texts})

    return results


# %%
pipe = pipeline("text-classification", model="vadis/xlm-roberta-large-finetuned-siel-md-english")

# %%
d_paper_pred = {}
for idx, ssoar_id in enumerate(l_valid_pub_ids[0:]):
    if ssoar_id not in d_paper_pred.keys():
        id = ssoar_id.replace('gesis-ssoar-', '')
        with open(dir_filtered_json_text + '/' + id + '.json', 'rb') as f_i:
            paper_json = orjson.loads(f_i.read())

        l_query = []

        idxcnt_sent = 0
        d_idx_pred = {}
        for section, sentences in paper_json.items():

            ssentences = [s[:512] for s in sentences]
            l_pred = pipe(ssentences)
            z_sp = zip(ssentences, l_pred)
            for (sent, pred) in z_sp:
                d_idx_pred[idxcnt_sent] = {'sentence': sent, 'pred': pred}
                idxcnt_sent += 1

        sum_pos = sum([1 for v in d_idx_pred.values() if v['pred']['label'] =='LABEL_1'])
        print(idx, ssoar_id, len(d_idx_pred), sum_pos)
        d_paper_pred[ssoar_id] = d_idx_pred


# %%
save_json(d_paper_pred, 'data/d_paper_pred.json')

# %%
with open(Path('data/d_paper_pred.json'), 'r') as f:
    d_paper_pred = json.load(f)

# %%
settings = d_svi_settings

all_results_dict = {}
cnt_err = 0


# %%
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
model, dim = load_model(
    settings['model_name_or_path'],
    device,
)

ext_docs = all_results_dict.keys()
for idx, ssoar_id in enumerate(l_valid_pub_ids[:]):
    id = ssoar_id.replace('gesis-ssoar-', '')
    print(ssoar_id, idx)
    # ssoar_id = pubs_corpus_ids[0]
    if ssoar_id not in ext_docs:
        try:
            corpus_path = 'corpus/zeros4_ner/' + ssoar_id + '.tsv'
            langs = [d_metadata[ssoar_id]['lang']]
            emb_path = make_embeddings_name(
                settings['root_dir'],
                ssoar_id + '_' + settings['model_name_or_path'],
                langs,
            )
            embeddings_dict = load_embeddings(
                corpus_path,
                langs,
                emb_path,
                settings['max_corpus_size'],
                model,
            )
            search_index_ = make_index(
                embeddings_dict['embeddings'],
                model[1].word_embedding_dimension,
                settings['n_clusters'],
                settings['nprobe']
            )

            '''paper_json = {}
            with open(dir_filtered_json_text + '/' + id + '.json', 'rb') as f_i:
                paper_json = orjson.loads(f_i.read())'''

            # l_ord_sentence1 = [(k, v['sentence']) for k, v in d_paper_pred[ssoar_id].items() if v['pred']['label']=='LABEL_1']

            l_ord_preds = [(k, v) for k, v in d_paper_pred[ssoar_id].items() if v['pred']['label'] == 'LABEL_1']

            l_query = []
            d_ord_clfs = {}
            for (ord, pred) in l_ord_preds:
                # print(round(logit(pred['pred']['score'])))
                d_ord_clfs[int(ord)] = logit(pred['pred']['score'])
                l_query.append(Query(text=pred['sentence'], order=int(ord)))

            '''for section, sentences in paper_json.items():
                for sentence in sentences:
                    l_query.append(Query(text= sentence, section= section))'''
            ql_s = QueryList(queries=l_query)

            results = search_batch(query=ql_s, search_index=search_index_, top_k=10)

            results_clfs = [res | {'clf_score': d_ord_clfs[res['query'].order]} for res in results]

            pos_results = [v for v in results if v['pred_label'] == 1]

            print(len(l_ord_preds), len(pos_results))

            # pos_results_dict[ssoar_id] = pos_results
            all_results_dict[ssoar_id] = results_clfs
        except Exception as err:
            # raise
            cnt_err += 1
            print(ssoar_id, err)
            pass
        print(f'{idx+1}, cnt_err: {cnt_err}')
    vadis_logger.info(f'svident_auto ran on {idx+1} docs, cnt_err: {cnt_err}')
# %%
pickle_.dump(all_results_dict, open('all_results_dict_v3.p', 'wb'))
# %%
with open('all_results_dict_v2.p', 'rb') as handle:
    all_results_dict_v2 = pickle.load(handle)

# %%

list_sw_en = stopwords.words('english')
list_sw_de = stopwords.words('german')
list_prop_pubs = []
d_zeros4 = {}
cnt_all_vsent = 0
for ssoar_id in l_valid_pub_ids[:]:
    try:
        print(ssoar_id)
        #list_prop_pubs.append(ssoar_id + ',\n')

        preds = all_results_dict[ssoar_id]
        dict_var_sentence = {}
        for s in preds:

            # print(ssoar_id)
            list_var = []
            if len(s['query'].text) >= 35:
                common_words = []

                dict_t_matchs = {}

                '''pos_score_ratio = s['pred_pos_score'] / (s['pred_pos_score'] + s['pred_neg_score'])
                norm_pos_score = (pos_score_ratio - 0.5) / (0.5)
                print(s['pred_pos_score'], s['pred_neg_score'], norm_pos_score)
                dict_t_matchs['score'] = str(round(pos_score_ratio, 2))'''
                dict_t_matchs['score'] = round(s['clf_score'])
                for m in s['top_k_texts']:
                    # if the match is a variable
                    '''if 'exploredata' in str(m[1]):
                        dict_t_matchs[m[1]] = m[0]

                        s0 = s['query'].text.lower()
                        s1 = m[0].lower()
                        s0List = s0.split(" ")
                        s1List = s1.split(" ")
                        common_words += list(set(s0List)&set(s1List))'''
                    if 1 == m[3]:
                        # dict_t_matchs[m[1]] = {'var_text': m[0], 'var_sim': m[2]}
                        list_var.append({m[1]: {'var_text': m[0], 'var_sim_score': round(100*m[2])}})
                        s0 = s['query'].text.lower()
                        s1 = m[0].lower()
                        s0List = s0.split(" ")
                        s1List = s1.split(" ")
                        common_words += list(set(s0List) & set(s1List))

                if d_metadata[ssoar_id]['lang'] == 'en':
                    filtered_common_words = list(set([cw for cw in common_words if cw not in list_sw_en and len(cw)>2]))
                if d_metadata[ssoar_id]['lang'] == 'de':
                    filtered_common_words = list(set([cw for cw in common_words if cw not in list_sw_de and len(cw)>2]))
                # print(filtered_common_words)

                dict_t_matchs['var_list'] = list_var
                dict_t_matchs['common_words'] = filtered_common_words
                dict_t_matchs['sentence'] = s['query'].text
                dict_var_sentence[s['query'].order] = dict_t_matchs

        d_zeros4[ssoar_id] = dict_var_sentence
        d_svident_auto[ssoar_id] = dict_var_sentence
        cnt_all_vsent += len(dict_var_sentence)
        '''with open('data/pubs_var_sentences_vars_v4_qu_ansner/' + ssoar_id + '_variables.json', 'w') as f:
            json.dump(dict_var_sentence, f)'''
    except Exception as err:
        print(ssoar_id, err)
        # raise

print(f'number of all variable sentences: {cnt_all_vsent}')
# %%
with open('varident_4_xrlfen_zeros4.json', 'w', encoding='utf8') as outfile:
    json.dump(d_zeros4, outfile)
# %%
with open('data/svident_auto.json', 'w', encoding='utf8') as outfile:
    json.dump(d_svident_auto, outfile)
# %%
process = 'SVIdent - Auto'
vadis_logger.info(f'PROCESS FINISHED: {process}.')
# %%
