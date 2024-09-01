import sqlite3

def get_sqlite_connection(project_name):
    conn = sqlite3.connect(f"{project_name}.db")
    return conn


import os
import pandas as pd
from py2neo import Graph


def process_project_files(project_name, repo_path, graph_conn: Graph):
    # 获取根目录文件信息
    df = get_root_files(project_name)

    # 连接 SQLite 数据库
    sqlite_conn = get_sqlite_connection(project_name)
    cursor = sqlite_conn.cursor()

    # 创建 get_brief_description 表(如果不存在)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS get_brief_description
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
     relative_path TEXT,
     description TEXT)
    ''')

    for _, row in df.iterrows():
        file_path = os.path.join(repo_path, row['relative_path'])

        # 读取文件内容
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # 调用大模型获取描述(这里假设您已经实现了这个函数)
        description = get_brief_description(content)

        # 存储到 SQLite
        cursor.execute('''
        INSERT INTO get_brief_description (relative_path, description)
        VALUES (?, ?)
        ''', (row['relative_path'], description))

        # 获取插入的 id
        sqlite_id = cursor.lastrowid

        # 更新图数据库
        update_query = f"""
        MATCH (f:{FILE_TYPE} {{id: $file_id}})
        SET f.description_id = $description_id
        """
        graph_conn.run(update_query, file_id=row['id'], description_id=sqlite_id)

    # 提交 SQLite 事务
    sqlite_conn.commit()
    sqlite_conn.close()


# 使用示例
if __name__ == "__main__":
    project_name = "example_project"
    repo_path = r"C:\workspace\example_project"
    graph_conn = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

    process_project_files(project_name, repo_path, graph_conn)