from openai import AzureOpenAI

client= AzureOpenAI(
    azure_endpoint="https://embedding-model-project.openai.azure.com/",
    api_key="8V56PzM9wT992AhHNiuJh0dUC41Qj3dWmCzY22xEYJgZI6QZcbI7JQQJ99BKACYeBjFXJ3w3AAABACOGVFRx",
    api_version="2024-02-01"
)

def get_embedding(text):
    res = client.embeddings.create(
        model = "embedding-model",
        input="Hello World"
    )
    return len(res.data[0].embedding)
print(get_embedding("Hello World"))