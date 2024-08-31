import time

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="http://127.0.0.1:12347/v1",
    model="codestral-22b",
    api_key="a",
    temperature=0.1,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)



class CodeSkimResponse(BaseModel):
    isImportant: bool = Field(description="是否包含独特的业务逻辑，一些不重要的代码文件，如 __init__.py 可认为 False")
    functions: str = Field(description="代码文件整体的功能描述")
    keyObjects: list = Field(description="可能包含业务逻辑的函数或者类名")

parser = JsonOutputParser(pydantic_object=CodeSkimResponse)

prompt = PromptTemplate(
    template="我现在想要阅读一个代码仓库，我将这个代码仓库相关文档与某个代码文件路径和内容发送给你，请你帮我判断这个代码文件是否重要以及其包含的一些基础信息。"
             "\n{format_instructions}\n##相关文档{relative_docs}\n##代码信息\n代码路径:{code_file_path}\n代码内容:{code_file_content}",
    input_variables=["relative_docs","code_file_path","code_file_content"],
    partial_variables={"format_instructions": parser.get_format_instructions()},
)

chain = prompt| llm


def process_file(rd, cfp, cfc):
    request = {"code_file_path":cfp}
    real_request = request.copy()
    real_request.update({"relative_docs":rd,
                         "code_file_content":cfc})

    rst = {'request_meta': request}
    try:
        start = time.time()
        raw_result = chain.invoke(real_request)
        elapsed_time = (time.time() - start) * 1000
        rst['elapsed_time'] = elapsed_time

        raw_content = raw_result.content
        rst['raw_content'] = raw_content
        token_usage = raw_result.response_metadata['token_usage']
        rst['token_usage'] = token_usage

        rst['structured'] = parser.parse(raw_content)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        return rst

