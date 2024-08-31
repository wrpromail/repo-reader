from openai import OpenAI

client = OpenAI(api_key="sk-gHOCMPiHAmjfwC9nFfFd9d59A3004bE9B056CaA97e65FcC0", base_url="https://api.oaipro.com/v1/")


def get_embedding(text, model="text-embedding-3-small"):
    text = text.replace("\n", " ")
    return client.embeddings.create(input=[text], model=model).data[0].embedding
