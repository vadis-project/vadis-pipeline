# %%
import logging
import json
import requests

from helper import check_json
from vadis_logger import vadis_logger
from config import config
# %%
process = 'retrieve pubs with related datasets'
vadis_logger.info(f'PROCESS STARTED: {process}.')

# %%    CONFIG
url = config['urls']['gesis_search']
l_lang = config['languages']

d_lang_query = dict()
for lang in l_lang:
    d_query = dict()
    d_query['body'] = config['queries']['gws_ssoar_withds'][lang]['body']
    d_query['result'] = config['queries']['gws_ssoar_withds'][lang]['result']
    d_lang_query[lang] = d_query

# %%

for lang in l_lang:
    query_body = d_lang_query[lang]['body']
    vadis_logger.info(f'{lang} query body for gws_ssoar_withds is loaded.')

    content = json.loads(requests.post(url, json=json.load(open(query_body, 'r'))).content)

    count_res = content['hits']['total']['value']
    vadis_logger.info(f'{count_res}: {lang} result size - result for gws_ssoar_withds is retrieved.')

    check_json(d_lang_query[lang]['result'], content['hits'])

# %%
vadis_logger.info(f'PROCESS FINISHED: {process}.')

# %%
