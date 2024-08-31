relative_docs = "这个代码仓库是一个用于管理用户知识的python后端，允许用户传递不同类型的数据，比如视频、pdf、ppt等，然后异步抽取其中的数据并做管理"

with open("sample_data.py", "r", encoding='utf-8') as inp:
    code_file_content = inp.read()

code_file_path = "apps/edu/curd/mysql_curd.py"
