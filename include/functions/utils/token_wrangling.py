import tiktoken

def count_tokens(text, model="gpt-4"):
    enc = tiktoken.encoding_for_model(model)
    return len(enc.encode(text))

def split_text_to_fit_tokens(text, max_tokens, model="gpt-4"):
    enc = tiktoken.encoding_for_model(model)
    tokens = enc.encode(text)
    chunks = []
    
    while len(tokens) > max_tokens:
        chunk = tokens[:max_tokens]
        chunks.append(enc.decode(chunk))
        tokens = tokens[max_tokens:]
    
    chunks.append(enc.decode(tokens))
    return chunks