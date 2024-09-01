from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import CommaSeparatedListOutputParser


output_parser = CommaSeparatedListOutputParser()

llm = ChatOpenAI(base_url="http://36.103.203.211:18534/v1/",api_key="a", model_name="llama3.1:8b", temperature=0.1)

target = ['SUPPORT.md', 'SECURITY_CONTACTS', 'README.md', 'OWNERS_ALIASES', 'OWNERS', 'Makefile', 'LICENSE', 'go.work.sum', 'go.work', 'go.sum', 'go.mod', 'CONTRIBUTING.md', 'code-of-conduct.md', 'CHANGELOG.md', '.go-version', '.gitignore', '.gitattributes', '.generated_files']
file_names = ", ".join(target)
# print(file_names)

format_instructions = output_parser.get_format_instructions()
prompt = PromptTemplate(
    template="##背景\n我现在需要你帮我摘要一个代码仓库的各项基本信息，我现在会给你传递其根目录下的文件名称，请根据文件名称告诉我你觉得哪些文件必须要查看，请不要回答其他无关内容."f"\n##文件名称列表\n{file_names}\n{format_instructions}",
    input_variables=["file_names"],
    partial_variables={"format_instructions": format_instructions})


chain = prompt | llm | output_parser

rst = chain.invoke({"file_names": file_names})
print(rst)