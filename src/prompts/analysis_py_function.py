import os

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM


template = """作为一位经验丰富的软件工程师和技术文档专家，请根据以下信息为给定的代码生成详细的说明和功能介绍：

    1. 项目主要用途: {project_brief}

    2. 代码文件路径: {code_path}

    3. 待分析实体类型: {code_type}
    
    4. 待分析的实体代码:
    {code}

    5. 同文件中其他实体与简介：
    {related_code}

    请提供以下内容：

    a) 代码概述：简要说明这段代码的主要功能和目的。

    b) 详细功能介绍：深入解释代码的具体功能，包括其在项目中的作用、主要算法或逻辑流程、输入输出等。

    c) 参数说明：如果是函数，解释每个参数的用途；如果是类，说明主要属性的含义。

    d) 返回值说明：解释函数或方法的返回值含义（如果适用）。

    e) 注意事项：指出使用这段代码时需要注意的关键点、潜在的陷阱或限制。

    f) 与其他组件的关系：解释这段代码与同文件中其他函数或类的关系，以及它在整个项目中的角色。

    请确保你的解释清晰、准确，并考虑到代码在整个项目中的上下文。如果有任何不明确的地方，请指出并提供合理的假设。"""

prompt = ChatPromptTemplate.from_template(template)

model = OllamaLLM(model=os.getenv("OLLAMA_MODEL", "codegeex4"))
chain = prompt | model

sample_code = """
def parse_document(file_details: dict):
    \"\"\"
    解析doc/docx
    :param: file_details: dict 包含file_id、path等关键参数
    \"\"\"
    clogger.info("解析文档内容")
    file_id = file_details.get("file_id")
    if file_details.get("type") == "doc":
        output_path = "/".join(file_details.get("path").split("/")[:-1])
        command = f'libreoffice --headless --convert-to docx --outdir {output_path} {file_details.get("path")}'
        subprocess.run(command, shell=True)
        file_details['path'] = file_details.get("path").split(".")[0] + ".docx"
    document = Document(file_details.get("path"))
    for paragraph in document.paragraphs:
        if paragraph.text != "":
            print(paragraph.text)
            save_content(file_id=file_id, content=paragraph.text)
    file_recognition_finish(file_id=file_id)
    return
"""

input_data = {
    "project_brief": "该项目是一个python后端服务，其为一个知识管理服务的组成部件",
    "code_path": "apps/edu/views/view_funcs.py",
    "code_type": "function",
    "code": sample_code,
    "related_code": ""
}

if __name__ == "__main__":
    result = chain.invoke(input_data)
    print(result)
