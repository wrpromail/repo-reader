import re
from pathlib import Path


def merge_requirements(file_paths):
    """
    合并多个requirements文件，去除重复包名。

    :param file_paths: 包含requirements文件路径的列表
    :return: 合并后的依赖列表
    """
    dependencies = set()

    for file_path in file_paths:
        path = Path(file_path)
        if not path.exists():
            print(f"Warning: File {file_path} does not exist. Skipping.")
            continue

        with path.open('r') as file:
            for line in file:
                # 去除注释和空行
                line = line.strip()
                if line and not line.startswith('#'):
                    # 提取包名，忽略版本信息
                    package_name = re.split(r'[=<>!~]=|[<>]', line)[0].strip()
                    dependencies.add(package_name)

    return sorted(list(dependencies))


# 使用示例
if __name__ == "__main__":
    files = ['path/to/requirements1.txt', 'path/to/requirements2.txt']
    merged_dependencies = merge_requirements(files)
    print("合并后的依赖列表：")
    for dep in merged_dependencies:
        print(dep)