#%%

import orjson
from tqdm import tqdm
from pathlib import Path
from lingua import Language, LanguageDetectorBuilder
from logger_and_config import logger, config
import json
import re
import json
import pickle
import orjson

from nerd import nerd_client
# %%    !!! ALREADY EXTRACCTED WITH SPACY  !!!

process = 'named entity extraction'
logger.info(f'PROCESS STARTED: {process}.')

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
# %%
entity_fisher = nerd_client.NerdClient()


def get_named_entities_of_sentence(sentence, entity_fisher, lang):
    try:
        entities = entity_fisher.disambiguate_text(sentence, language=lang)[0]['entities']
    except:
        print('Error occured for: ' + sentence)
        entities = []
    entity_list = []
    for entity in entities:
        #print(entity)
        if 'wikipediaExternalRef' in entity:
            wikipedia_id = entity['wikipediaExternalRef']
            entity_list.append(wikipedia_id)
        if 'wikidataId' in entity:
            wikidata_id = entity['wikidataId']
            entity_list.append(wikidata_id)
    return [e for e in entities if 'wikidataId' in e] # entity_list


# %%    DOC ENTITIES
d_doc_sent_ners = {}
for id in l_valid_pub_ids[:]:

    print(id)

    try:
        lang = d_metadata[id]['lang']
        related_research_datasets = d_metadata[id]['available_related_research_datasets_list']

        col_id = []
        col_sentence = []
        col_is_variable = []
        col_lang = []
        col_variable = []
        col_uuid = []

        d_sent_ners = {}


        for vocab in related_research_datasets:
            print(vocab)
            with open('sampled_vocab/' + vocab + '.json', 'r', encoding='utf8') as f:
                loaded_dict = json.load(f)
                #print(loaded_dict)

                for k in loaded_dict[lang].keys():
                    print(k)
        d_doc_sent_ners[id] = d_sent_ners
    except:
        raise
        print('not found', id)
# %%
'''with open('d_doc_sent_ners.json', 'w') as f:
    json.dump(d_doc_sent_ners, f)'''
# %%
# %%    CORPUS ENTITIES
pubs_corpus_ids = l_valid_pub_ids
pubs_corpus = d_metadata

d_corpus_sent_ners = {}
for idx, id in enumerate(pubs_corpus_ids[:]):

    print(idx, id)

    try:
        related_research_datasets = pubs_corpus[id]['available_related_research_datasets_list']
        lang_pub = pubs_corpus[id]['lang']
        col_id = []
        col_sentence = []
        col_is_variable = []
        col_lang = []
        col_variable = []
        col_uuid = []
        sentence_tokens = []

        '''with open('./6_sentence_json/' + id + '.json', "rb") as f_i:
            paper_json = orjson.loads(f_i.read())
            sentence_tokens = paper_json['sentences']'''

        '''for s in sentence_tokens:
            l_ner = get_named_entities_of_sentence(s, entity_fisher)

            d_sent_ners[s] = l_ner'''
            #print(l_ner)

        for vocab in related_research_datasets:
            print(vocab)

            if vocab not in d_corpus_sent_ners.keys():

                d_sent_ners = {}
                with open('./sampled_vocab/' + vocab + '.json', 'r') as f:
                    loaded_dict = json.load(f)
                    # print(loaded_dict)

                    for k in loaded_dict.keys():
                        l_ner = get_named_entities_of_sentence(k, entity_fisher, lang_pub)

                        d_sent_ners[k] = l_ner

                d_corpus_sent_ners[vocab] = d_sent_ners

            else:
                print(f'{vocab} is already processed for NER extraction.')


    except:
        raise
        print('not found', id)
# %%
'''with open('d_corpus_sent_ners.json', 'w') as f:
    json.dump(d_corpus_sent_ners, f)'''
# %%    ENTITY MATCHING

with open('d_doc_sent_ners.json', 'r') as f:
    d_doc_sent_ners = json.load(f)

with open('d_corpus_sent_ners.json', 'r') as f:
    d_corpus_sent_ners = json.load(f)
# %%
d_doc_var_sent_ner = {}
for idx, id in enumerate(pubs_corpus_ids[:]):

    print(idx, id)

    try:

        related_research_datasets = pubs_corpus[id]['related_research_datasets']

        d_doc_ners = d_doc_sent_ners[id]

        sentence_tokens = []
        d_var_sent = {}

        with open('./6_sentence_json/' + id + '.json', "rb") as f_i:
            paper_json = orjson.loads(f_i.read())
            sentence_tokens = paper_json['sentences']

        '''for s in sentence_tokens:
            l_ner = get_named_entities_of_sentence(s, entity_fisher)

            d_sent_ners[s] = l_ner'''
            #print(l_ner)

        for vocab in related_research_datasets:
            print(vocab)

            d_vocab_ners = d_corpus_sent_ners[vocab]

            with open('../unified_vocab/' + vocab + '.pkl', 'rb') as f:
                loaded_dict = pickle.load(f)

            for s, v_s_ners in d_doc_ners.items():
                s_doc_nerids = set([ner['wikidataId'] for ner in v_s_ners])
                for v, v_v_vers in d_vocab_ners.items():
                    s_var_nerids = set([ner['wikidataId'] for ner in v_v_vers])
                    inter_ners = s_doc_nerids.intersection(s_var_nerids)
                    if(len(inter_ners) > 2):
                        print(1, s)
                        print(2, v)
                        #print(inter_ners)
                        common_ners = [ner for ner in v_s_ners if ner['wikidataId'] in inter_ners]
                        d_match = {'var_text': v, 'sentence': s, 'sentence_id': sentence_tokens.index(s), 'rd_id': vocab, 'var_ids': loaded_dict[v]['var_ids'], 'common_ners' : common_ners}
                        #print(d_match)
                        d_var_sent[v] = d_match
        d_doc_var_sent_ner[id] = d_var_sent            

    except:
        #raise
        print('not found', id)
# %%
