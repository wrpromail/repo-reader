### 待实现功能


### 待思考问题
1. 使用 golang 处理写入和关系建立，相对 python 速度能快多少？
2. 仅 kubernetes 一个项目的文件、目录关系，就占用了大量内存，后续再添加代码对象关系、对象社区信息，该如何控制内存使用？


### 性能优化
```python
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

def parallel_batch_process(repo_input):
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        if isinstance(repo_input, dict):
            futures = [executor.submit(process_repository, repo_path, repo_label) for repo_label, repo_path in repo_input.items()]
        elif isinstance(repo_input, list):
            futures = [executor.submit(process_repository, repo_path) for repo_path in repo_input]
        elif isinstance(repo_input, str):
            futures = [executor.submit(process_repository, repo_input)]
        
        for future in futures:
            future.result()
```

```python
def batch_create_nodes(nodes):
    query = """
    UNWIND $nodes AS node
    CREATE (n:File)
    SET n += node
    """
    graph.run(query, nodes=nodes)

# 在处理文件时，收集节点信息
nodes_to_create = []
for file_path in tqdm(files_to_process, desc="Processing files"):
    # ... (创建节点信息)
    nodes_to_create.append(node_info)
    if len(nodes_to_create) >= 1000:
        batch_create_nodes(nodes_to_create)
        nodes_to_create = []

# 处理剩余的节点
if nodes_to_create:
    batch_create_nodes(nodes_to_create)
```


```bash
docker run --name neo4j -p 7474:7474 -p 7687:7687 -d -v $HOME/neo4j/data:/data -v $HOME/neo4j/logs:/logs -v $HOME/neo4j/import:/var/lib/neo4j/import -v $HOME/neo4j/plugins:/plugins --env NEO4J_AUTH=neo4j/password --env NEO4J_dbms_memory_heap_initial__size=4G --env NEO4J_dbms_memory_heap_max__size=8G --env NEO4J_dbms_memory_pagecache_size=4G --cpus 6 neo4j:latest
```