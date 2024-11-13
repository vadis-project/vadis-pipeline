# %%
from bs4 import BeautifulSoup
import json
import requests
from pathlib import Path
import time

from helper import save_json
from vadis_logger import vadis_logger
from config import config
# %%
process = 'download PDF files of documents'
vadis_logger.info(f'PROCESS STARTED: {process}.')

l_lang = config['languages']
f_pub_list = Path('vadis_app_ssoar_list.json')
f_metadata = Path('metadata.json')

# %%
with open(f_metadata, 'r') as f:
    d_metadata = json.load(f)

l_id = []
with open(f_pub_list, 'r') as f:
    d_pub_list = json.load(f)
if 'ssoar_ids' in d_pub_list:
    l_id = d_pub_list['ssoar_ids'] # remove gesis-ssoar-


# %%
def download_ssoar_pdf_list(l_ssoar, p_output_dir):

    for ssoar_id in l_ssoar:
        id = ssoar_id.split('-')[2]
        filename = Path(p_output_dir + str(id) + '.pdf')
        d_metadata[ssoar_id]['downloaded'] = False
        if filename.exists():
            vadis_logger.info(f'{id} already exists.')
            d_metadata[ssoar_id]['downloaded'] = True
        else:
            vadis_logger.info(f'{id} will be downloaded.')
            page = requests.get('https://www.ssoar.info/ssoar/handle/document/' + id )
            soup = BeautifulSoup(page.content, 'html.parser')
            links = soup.find_all('a')
            # print(links)
            for link in links:
                try:
                    link_url = link['href']
                    if 'pdf' in link_url:
                        vadis_logger.debug(f'{id} link found: {link_url}')

                        url = link_url
                        response = requests.get(url)
                        filename.write_bytes(response.content)
                        time.sleep(10)
                        vadis_logger.debug(f'{id} SUCCESSFULLY download.')
                        d_metadata[ssoar_id]['downloaded'] = True
                except Exception as err:
                    vadis_logger.error(err)
                    # raise
                    continue
                    vadis_logger.debug('{id} FAILED download')
            if d_metadata[ssoar_id]['downloaded']==False:
                vadis_logger.debug(f'{id} NO link found.')


# %%
download_ssoar_pdf_list(l_id[:], 'corpus/pdf_raw/')
# %%
save_json(d_metadata, 'metadata.json')

# %%
vadis_logger.info(f'PROCESS FINISHED: {process}.')
