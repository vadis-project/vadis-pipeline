from sentence_transformers import SentenceTransformer


def load_model(model_name_or_path, device):
    model = SentenceTransformer(model_name_or_path, device=device)
    dim = model[1].word_embedding_dimension
    return model, dim
