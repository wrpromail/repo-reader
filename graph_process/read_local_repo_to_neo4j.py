import os
import uuid
import fnmatch
from tqdm import tqdm
from py2neo import Node, Relationship, Graph
from typing import Union, Dict, List

from graph_db_conn import graph
from constants import *
from helper import get_project_name


repo_path = r"C:\workspace\spark"
project_name = get_project_name(repo_path)


# 排除目录列表
exclude_dirs = default_exclude


def should_exclude(path):
    """检查是否应该排除该路径"""
    return any(excluded in path.split(os.sep) for excluded in exclude_dirs)

def parse_gitignore(repo_path):
    """解析.gitignore文件"""
    gitignore_path = os.path.join(repo_path, '.gitignore')
    ignore_patterns = []
    if os.path.exists(gitignore_path):
        try:
            with open(gitignore_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        ignore_patterns.append(line)
        except IOError as e:
            print(f"Error reading .gitignore file: {e}")
    print("Ignore patterns:", ignore_patterns)
    return ignore_patterns

def should_ignore(path, repo_path, ignore_patterns):
    """检查是否应该忽略该路径"""
    relative_path = os.path.relpath(path, repo_path)
    return any(fnmatch.fnmatch(relative_path, pattern) for pattern in ignore_patterns)


def scan_directory(root_path):
    """扫描目录，返回所有需要处理的文件和目录"""
    files_to_process = []
    dirs_to_process = []
    ignore_patterns = parse_gitignore(root_path)
    for root, dirs, files in os.walk(root_path, topdown=True):
        # 修改 dirs 列表来排除不想遍历的目录
        dirs[:] = [d for d in dirs if
                   d not in exclude_dirs and not should_ignore(os.path.join(root, d), root_path, ignore_patterns)]

        if not any(excluded in root.split(os.sep) for excluded in exclude_dirs) and not should_ignore(root, root_path,
                                                                                                      ignore_patterns):
            dirs_to_process.append(root)
            for file in files:
                file_path = os.path.join(root, file)
                if not should_ignore(file_path, root_path, ignore_patterns):
                    files_to_process.append(file_path)

    return dirs_to_process, files_to_process


def create_file_node(file_path, repo_root, level, project_name):
    """创建文件节点"""
    relative_path = os.path.relpath(file_path, repo_root)
    file_name = os.path.basename(file_path)
    file_ext = os.path.splitext(file_name)[1]
    file_id = str(uuid.uuid4())

    # 创建Neo4j节点
    node = Node(FILE_TYPE,
                id=file_id,
                name=file_name,
                relative_path=relative_path,
                type=FILE_TYPE,
                extension=file_ext,
                project_name=project_name,
                level=level)

    return node


def create_directory_node(dir_path: str, repo_root: str, level: int, project_name: str) -> Node:
    """创建目录节点"""
    relative_path = os.path.relpath(dir_path, repo_root)
    dir_name = os.path.basename(dir_path)
    return Node(DIRECTORY_TYPE,
                id=str(uuid.uuid4()),
                name=dir_name,
                relative_path=relative_path,
                type=DIRECTORY_TYPE,
                project_name=project_name,
                level=level)


def process_repository(target_repo_path: str, repo_label: str = None):
    print(f"Processing repository: {target_repo_path}")
    if repo_label is None:
        repo_label = get_project_name(target_repo_path)

    # 使用 repo_label 作为 project_name
    project_name = repo_label

    # 扫描目录
    dirs_to_process, files_to_process = scan_directory(target_repo_path)

    # 创建根目录节点
    root_node = create_directory_node(target_repo_path, target_repo_path, level=0, project_name=project_name)
    root_node['repo_label'] = repo_label
    graph.create(root_node)

    # 处理目录
    dir_nodes = {target_repo_path: root_node}
    for dir_path in tqdm(dirs_to_process, desc="Processing directories"):
        if dir_path == target_repo_path:
            continue
        level = len(os.path.relpath(dir_path, target_repo_path).split(os.sep))
        dir_node = create_directory_node(dir_path, target_repo_path, level, project_name=project_name)
        dir_node['repo_label'] = repo_label
        graph.create(dir_node)
        dir_nodes[dir_path] = dir_node

        # 创建父子关系
        parent_path = os.path.dirname(dir_path)
        if parent_path in dir_nodes:
            graph.create(Relationship(dir_nodes[parent_path], "CONTAINS", dir_node))

    # 处理文件
    for file_path in tqdm(files_to_process, desc="Processing files"):
        level = len(os.path.relpath(os.path.dirname(file_path), target_repo_path).split(os.sep))
        file_node = create_file_node(file_path, target_repo_path, level, project_name=project_name)
        file_node['repo_label'] = repo_label
        graph.create(file_node)

        # 创建文件与目录的关系
        parent_dir = os.path.dirname(file_path)
        if parent_dir in dir_nodes:
            graph.create(Relationship(dir_nodes[parent_dir], "CONTAINS", file_node))
    print(f"Repository {target_repo_path} processed successfully.")


def batch_process_repositories(repo_input: Union[str, List[str], Dict[str, str]]):
    """批量处理多个仓库"""
    if isinstance(repo_input, str):
        repo_input = [repo_input]

    if isinstance(repo_input, list):
        for repo_path in repo_input:
            try:
                process_repository(repo_path)
                print(f"Successfully processed: {repo_path}")
            except Exception as e:
                print(f"Error processing {repo_path}: {str(e)}")
    elif isinstance(repo_input, dict):
        for repo_label, repo_path in repo_input.items():
            try:
                process_repository(repo_path, repo_label)
                print(f"Successfully processed: {repo_path} with label {repo_label}")
            except Exception as e:
                print(f"Error processing {repo_path}: {str(e)}")
    else:
        raise ValueError("Invalid input type. Expected string, list, or dictionary.")



# 主程序
if __name__ == "__main__":
    batch_job_list = [ "C:\\workspace\\kubernetes","C:\\workspace\\ansible","C:\\workspace\\spark",
                      "C:\\workspace\\tensorflow", "C:\\workspace\\pytorch", "C:\\workspace\\react"]
    batch_process_repositories(batch_job_list)
