from langchain_openai import ChatOpenAI
from langchain_core.prompts import PromptTemplate
from langchain_core.messages import AIMessage
from tqdm import tqdm
import os
from query_to_df import query_by_sql, graph, PROJECT_ROOT_FILES_SQL

llm = ChatOpenAI(base_url="http://127.0.0.1:11434/v1/",api_key="a", model_name="codestral", temperature=0.1)

template = """## 背景
我想了深入学习了解一个代码仓库的所有细节，以便我能完成快速开发或在实际项目有效地使用该仓库的制品。
## 目标
首先我会阅读代码仓库根目录下的各种文件，获取这个仓库的用途、依赖、构建、运行等信息。我会给你发送其中一个文件的名称与内容，你需要简要回答这个文件是否包含我所关注的信息。
若你觉得这个文件的内容没有需要关注的内容，则回答None即可。
每个文件使用一句话回答即可，不要重复原文件内容，也不要做过多无关的解释。
## 数据
根目录文件名: {root_file_list}
目标文件名: {target_file_name}
目标文件内容: {target_file_content}
"""

# 1. codestral 效果最好
# 2. 还是需要排除一些文件，比如 go.sum OWNER

prompt = PromptTemplate(template=template, input_variables=["root_file_list", "target_file_name", "target_file_content"])

chain = prompt | llm

df = query_by_sql(graph, PROJECT_ROOT_FILES_SQL, {"project_name": "kubernetes"})

def read_file_content(base_path, relative_path):
    full_path = os.path.join(base_path, relative_path)
    try:
        with open(full_path, 'r', encoding='utf-8') as file:
            return file.read()
    except Exception as e:
        print(f"Error reading file {full_path}: {e}")
        return ""

def process_root_files(df, base_path, llm, prompt):
    root_file_list = ", ".join(df['name'].tolist())
    results = {}

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing files"):
        file_name = row['name']
        file_content = read_file_content(base_path, row['relative_path'])

        if file_content:
            try:
                result = chain.invoke({
                    "root_file_list": root_file_list,
                    "target_file_name": file_name,
                    "target_file_content": file_content
                })
                # 如果结果是 AIMessage，提取其内容
                if isinstance(result, AIMessage):
                    result = result.content
                results[file_name] = result
            except Exception as e:
                print(f"Error processing {file_name}: {e}")
                results[file_name] = "Error: " + str(e)
        else:
            results[file_name] = "Error: Could not read file content"

    return results

def summarize_results(results):
    summary = "根目录文件内容汇总：\n\n"
    for file_name, content in results.items():
        if isinstance(content, AIMessage):
            content = content.content  # 获取 AIMessage 的实际内容
        if content != "None" and not (isinstance(content, str) and content.startswith("Error:")):
            summary += f"文件名: {file_name}\n"
            summary += f"内容摘要: {content}\n\n"
    return summary

if __name__ == "__main__":
    base_path = "C:\\workspace\\kubernetes"
    results = process_root_files(df, base_path, llm, prompt)
    # 汇总结果
    summary = summarize_results(results)
    print(summary)
