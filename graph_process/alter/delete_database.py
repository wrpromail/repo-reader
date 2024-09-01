from py2neo import Graph


def delete_database(uri, user, password, db_name):
    system_graph = Graph(uri, auth=(user, password), name="system")

    try:
        # 停止数据库
        system_graph.run(f"STOP DATABASE {db_name}")
        # 删除数据库
        system_graph.run(f"DROP DATABASE {db_name}")
        print(f"Database {db_name} has been deleted")
    except Exception as e:
        print(f"Error deleting database {db_name}: {str(e)}")


# 使用示例
uri = "bolt://localhost:7687"
user = "neo4j"
password = "your_password"
db_name = "langchain"

# 删除数据库
delete_database(uri, user, password, db_name)