from constants import FILE_TYPE, DIRECTORY_TYPE
from graph_db_conn import graph


def create_same_directory_relationships(project_name):
    """创建同级文件关系"""
    print(f"Creating relationships between files in the same directory for project: {project_name}")
    query = f"""
    MATCH (d:{DIRECTORY_TYPE}{{project_name: $project_name}})-[:CONTAINS]->(f1:{FILE_TYPE})
    MATCH (d)-[:CONTAINS]->(f2:{FILE_TYPE})
    WHERE f1 <> f2 AND f1.project_name = $project_name AND f2.project_name = $project_name
    MERGE (f1)-[:SAME_DIRECTORY]-(f2)
    """
    graph.run(query, project_name=project_name)


# 如果你想添加更多字段,可以这样修改函数:
def _create_same_directory_relationships(project_name, additional_fields=None):
    """创建同级文件关系"""
    print(f"Creating relationships between files in the same directory for project: {project_name}")

    match_conditions = [f"d.project_name = $project_name",
                        "f1 <> f2",
                        "f1.project_name = $project_name",
                        "f2.project_name = $project_name"]

    if additional_fields:
        for field in additional_fields:
            match_conditions.append(f"f1.{field} = f2.{field}")

    where_clause = " AND ".join(match_conditions)

    query = f"""
    MATCH (d:{DIRECTORY_TYPE})-[:CONTAINS]->(f1:{FILE_TYPE})
    MATCH (d)-[:CONTAINS]->(f2:{FILE_TYPE})
    WHERE {where_clause}
    MERGE (f1)-[:SAME_DIRECTORY]-(f2)
    """
    graph.run(query, project_name=project_name)