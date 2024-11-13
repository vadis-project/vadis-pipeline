import os
import faiss
import pickle
import numpy as np
import pandas as pd
from uuid import uuid4


def load_data(path, langs):
    df = pd.read_csv(path, sep='\t')
    if 'uuid' not in df.columns:
        df['uuid'] = [uuid4() for _ in range(df.shape[0])]
        # df.to_csv(path, index=False, sep='\t')
    df = df[df['lang'].isin(langs)]  # filter relevant language
    return df


def make_embeddings_name(root_dir, model_name, langs):
    os.makedirs(root_dir, exist_ok=True)
    file_name = f'{model_name}__{"-".join(langs)}.pkl'.replace('/', '--')
    return os.path.join(root_dir, file_name)


def load_embeddings(corpus_path, langs, emb_path, max_corpus_size, model):
    if not os.path.isfile(emb_path):
        print('Computing embeddings...')
        df = load_data(corpus_path, langs)

        corpus_sentences = []
        corpus_labels = []
        corpus_uuids = []
        corpus_ids = []

        for i in range(df.shape[0]):
            row = df.iloc[i]
            if row['sentence'] not in corpus_sentences:
                corpus_sentences.append(row['sentence'])
                corpus_labels.append(row['is_variable'])
                corpus_uuids.append(row['uuid'])
                corpus_ids.append(row['id'])

                if len(corpus_sentences) >= max_corpus_size:
                    break

        print('Encode the corpus. This might take a while')
        corpus_embeddings = model.encode(corpus_sentences, show_progress_bar=True, convert_to_numpy=True)

        # TODO: replace unsafe pickle with protobuf or docarray (https://github.com/docarray/docarray)
        print('Store file on disc')
        with open(emb_path, 'wb') as fOut:
            pickle.dump({'sentences': corpus_sentences, 'embeddings': corpus_embeddings, 'labels': corpus_labels, 'uuids': corpus_uuids, 'ids': corpus_ids}, fOut)
    else:
        print('Loading pre-computed embeddings...')
        with open(emb_path, 'rb') as fIn:
            cache_data = pickle.load(fIn)
            corpus_sentences = cache_data['sentences']
            corpus_embeddings = cache_data['embeddings']
            corpus_labels = cache_data['labels']
            corpus_uuids = cache_data['uuids']
            corpus_ids = cache_data['ids']
        print('Done.')

    return {
        'sentences': corpus_sentences, 
        'embeddings': corpus_embeddings, 
        'labels': corpus_labels, 
        'uuids': corpus_uuids,
        'ids': corpus_ids,
    }


def make_index(embeddings, dim, n_clusters, nprobe):
    quantizer = faiss.IndexFlatIP(dim)
    index = faiss.IndexIVFFlat(quantizer, dim, n_clusters, faiss.METRIC_INNER_PRODUCT)
    index.nprobe = nprobe

    # normalize vectors to unit length
    embeddings = embeddings / np.linalg.norm(embeddings, axis=1)[:, None]

    # train index to find suitable clustering
    index.train(embeddings)

    # add all embeddings to index
    index.add(embeddings)

    return index
