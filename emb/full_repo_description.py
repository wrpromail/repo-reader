import os
import json
from tqdm import tqdm
from py_secure import generate_code_documentation


def generate_repository_documentation(repo_path, output_file, project_brief):
    # 获取仓库中所有的Python文件
    python_files = []
    for root, dirs, files in os.walk(repo_path):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))

    # 创建进度条
    progress_bar = tqdm(total=len(python_files), desc="Generating Documentation")

    # 打开输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        # 遍历每个Python文件
        for file_path in python_files:
            try:
                # 生成文档
                documentation_list = generate_code_documentation(file_path, project_brief)
                if not documentation_list or len(documentation_list) == 0:
                    continue

                # 遍历每个实体的文档
                for doc in documentation_list:
                    # 创建输出对象
                    output = {
                        "file_path": file_path,
                        "entity_name": doc['name'],
                        "entity_type": doc['type'],
                        "documentation": doc['documentation']
                    }

                    # 将输出写入JSONL文件
                    json.dump(output, f, ensure_ascii=False)
                    f.write('\n')

            except Exception as e:
                print(f"Error processing file {file_path}: {str(e)}")

            # 更新进度条
            progress_bar.update(1)

    # 关闭进度条
    progress_bar.close()

    print(f"Documentation generation complete. Results saved to {output_file}")


if __name__ == "__main__":
    repo_path = "/Users/wangrui/zhipu/wanjuan/kg_system"
    output_file = "kg_system_2.jsonl"
    project_brief = "该项目是一个python后端服务，其为一个知识管理服务的组成部件"

    generate_repository_documentation(repo_path, output_file, project_brief)