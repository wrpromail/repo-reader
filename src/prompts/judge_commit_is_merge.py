import os

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM


template = """
以下是某仓库的git提交信息，由你判断是否是 Merge Request 或其他系统自动创建的提交，你只需要回答 yes 或 no，不要回答其他无关信息。
提交信息:
{commit_message}
"""

prompt = ChatPromptTemplate.from_template(template)

model = OllamaLLM(model=os.getenv("OLLAMA_MODEL", "codegeex4"))
chain = prompt | model

sample_message1 = """
Merge branch 'fix-app-image' into 'develop'
    
    更新 python requests 依赖与在edu-backend 组件的dockerfile 中添加pip 更新
    
    See merge request kg-m/kg-edu/eduplatform-backend!150

"""


sample_message2 = """
更新 python requests 依赖与在edu-backend 组件的dockerfile 中添加pip 更新
"""

if __name__ == "__main__":
    result1 = chain.invoke({"commit_message": sample_message1})
    print(result1)

    result2 = chain.invoke({"commit_message": sample_message2})
    print(result2)