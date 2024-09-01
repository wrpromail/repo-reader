from graph_db_conn import graph

def delete_project_nodes(project_name: str):
    """删除指定项目的所有节点和关系"""
    query = f"""
    MATCH (n {{project_name: $project_name}})
    DETACH DELETE n
    """
    graph.run(query, project_name=project_name)
    print(f"Deleted all nodes and relationships for project: {project_name}")


if __name__ == "__main__":
    project_name = "test"
    delete_project_nodes(project_name)