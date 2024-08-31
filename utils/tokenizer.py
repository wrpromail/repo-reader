from transformers import AutoTokenizer

# 加载本地tokenizer
tokenizer_path = "../codestral"  # 请根据实际情况修改路径
tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)


def get_token_length(text):
    # 将文本编码为tokens
    tokens = tokenizer.encode(text)

    # 返回tokens的长度
    return len(tokens)


# 示例使用
text = "Explain Machine Learning to me in a nutshell."
token_length = get_token_length(text)
print(f"The text '{text}' has {token_length} tokens.")

# 测试多个文本
texts = [
    "This is a short sentence.",
    "Python is a programming language.",
    "Artificial Intelligence is changing the world in many ways.",
    # 添加更多你想测试的文本...
]

for text in texts:
    print(f"The text '{text}' has {get_token_length(text)} tokens.")
