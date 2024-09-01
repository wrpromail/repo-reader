import os

from neo4j import GraphDatabase
from py2neo import Graph


def get_or_create_database(uri, user, password, repo_path):
    db_name = os.path.basename(repo_path)

    # 使用Neo4j驱动程序来创建数据库
    driver = GraphDatabase.driver(uri, auth=(user, password))
    with driver.session(database="system") as session:
        # 检查数据库是否存在
        result = session.run("SHOW DATABASES")
        databases = [record["name"] for record in result]

        if db_name not in databases:
            # 创建新数据库
            session.run(f"CREATE DATABASE {db_name}")
            print(f"Created new database: {db_name}")
        else:
            print(f"Database {db_name} already exists")

    driver.close()

    # 返回py2neo的Graph对象，连接到特定数据库
    return Graph(uri, auth=(user, password), name=db_name)