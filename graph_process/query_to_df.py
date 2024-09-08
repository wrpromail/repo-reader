import pandas as pd
from graph_db_conn import graph
from constants import FILE_TYPE, DIRECTORY_TYPE

def execute_query(graph, query, params=None):
    result = graph.run(query, parameters=params)
    records = [record.values() for record in result]
    columns = result.keys()
    return pd.DataFrame(records, columns=columns)

def query_by_sql(graph, sql, params=None):
    df = execute_query(graph, sql, params)
    print(df.to_markdown(index=False))
    return df

def query_file(graph, match_dict, where_dict, return_dict):
    query = f"""
    MATCH {match_dict['pattern']}
    WHERE {where_dict['condition']}
    RETURN {', '.join([f"{k} as {v}" for k, v in return_dict.items()])}
    """
    params = where_dict['params']
    df = execute_query(graph, query, params)
    print(df.to_markdown(index=False))
    return df

def query_raw(graph, query, params=None):
    df = execute_query(graph, query, params)
    print(df.to_markdown(index=False))
    return df



PROJECT_ROOT_FILES_SQL = f"""
    MATCH (d:{DIRECTORY_TYPE} {{project_name: $project_name}})-[:CONTAINS]->(f:{FILE_TYPE})
    WHERE NOT ()-[:CONTAINS]->(d)
    RETURN f.id AS id, f.name AS name, f.relative_path AS relative_path
    """

# 搜索 kubernetes 项目中，目录下没有其他目录的文件夹，且包含的文件大于5个，并且这些文件名中没有 test 关键字。
query1 = """MATCH (d)
WHERE d.project_name = 'kubernetes' AND d.type = 'Directory'
AND NOT (d)-[:CONTAINS]->({type: 'Directory'})
WITH d
MATCH (d)-[:CONTAINS]->(f)
WHERE f.type = 'File' AND NOT f.name CONTAINS 'test'
WITH d, COLLECT(f) AS files
WHERE SIZE(files) > 5
RETURN d.name AS directory_name, d.relative_path AS directory_path, SIZE(files) AS file_count
LIMIT 10"""

# 目录下非代码文件数远大于代码文件数（对kubernetes项目，就是 .go 结尾的文件)，这里的远大于意思是占比总数80%以上。
query2 = """MATCH (d)
WHERE d.project_name = 'kubernetes' AND d.type = 'Directory'
MATCH (d)-[:CONTAINS]->(f)
WHERE f.type = 'File'
WITH d,
     COLLECT(CASE WHEN f.extension = '.go' THEN f END) AS code_files,
     COLLECT(CASE WHEN f.extension <> '.go' THEN f END) AS non_code_files
WHERE SIZE(non_code_files) > 0 AND 
      SIZE(code_files) > 0 AND
      toFloat(SIZE(non_code_files)) / (SIZE(code_files) + SIZE(non_code_files)) >= 0.8
RETURN d.name AS directory_name,
       d.relative_path AS directory_path,
       SIZE(code_files) AS code_file_count,
       SIZE(non_code_files) AS non_code_file_count,
       toFloat(SIZE(non_code_files)) / (SIZE(code_files) + SIZE(non_code_files)) AS non_code_ratio
ORDER BY non_code_ratio DESC
LIMIT 10"""

# 比如我想搜索 kubernetes 项目下名称为xx或名称包含xx的目录，并返回目录名、相对路径、其包含的文件数与目录数，请给出这个查询语句。
query3="""MATCH (d:Directory)
WHERE d.project_name = 'kubernetes' 
  AND (d.name = 'fieldpath' OR d.name CONTAINS 'fieldpath')
WITH d
OPTIONAL MATCH (d)-[:CONTAINS]->(f:File)
WITH d, COUNT(f) AS file_count
OPTIONAL MATCH (d)-[:CONTAINS]->(subdir:Directory)
WITH d, file_count, COUNT(subdir) AS subdir_count
RETURN DISTINCT d.name AS directory_name,
       d.relative_path AS directory_path,
       file_count,
       subdir_count"""

if __name__ == "__main__":
    query_by_sql(graph, query3)
    # df = query_by_sql(graph, PROJECT_ROOT_FILES_SQL, {"project_name": "kubernetes"})