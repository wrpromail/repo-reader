import os

from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama.llms import OllamaLLM
from py_split import split_code


def generate_entity_documentation(entity, project_brief, file_path, related_code):
    template = """作为一位经验丰富的软件工程师和技术文档专家，请根据以下信息为给定的代码生成详细的说明和功能介绍：

    1. 项目主要用途：{project_brief}

    2. 代码文件路径：{code_path}

    3. 待分析的函数或类：
    {code}

    4. 同文件中其他相关函数或类：
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

    input_data = {
        "project_brief": project_brief,
        "code_path": file_path,
        "code": entity['code'],
        "related_code": related_code
    }

    result = chain.invoke(input_data)
    return result


def generate_code_documentation(file_path, project_brief):
    try:
        entities = split_code(file_path)

        related_code = ""
        results = []

        for entity in entities:
            documentation = generate_entity_documentation(entity, project_brief, file_path, related_code)
            results.append({
                'name': entity['name'],
                'type': entity['type'],
                'code': entity['code'],
                'documentation': documentation,
            })

            # 更新 related_code
            # 存在问题，多个实体时， related_code 添加的内容应该是精简后的内容
            related_code += f"{entity['type'].capitalize()} {entity['name']}: {documentation}\n\n"

        return results
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []


if __name__ == "__main__":
    project_brief = "该项目是一个 python 后端，用途是接收前端请求与下发异步任务"
    file_path = '/Users/wangrui/zhipu/eduplatform-backend/apps/edu/curd/mysql_curd.py'
    entities = split_code(file_path)
    rst = generate_entity_documentation(entities[0], project_brief, file_path, "暂无")
    print(rst)
