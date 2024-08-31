from llm.plain_read import process_file
from utils.paths import get_code_files

import os
import json
import time
from tqdm import tqdm


def process_repository(label, repo_path, repo_description):
    # 获取所有代码文件
    code_files = get_code_files(repo_path)
    total_files = len(code_files)

    # 准备输出文件
    output_file = f"repo_analysis_{label}_{int(time.time())}.jsonl"

    # 记录开始时间
    start_time = time.time()

    # 初始化进度条
    with tqdm(total=total_files, desc="Processing files") as pbar:
        # 处理每个文件
        for file_path in code_files:
            full_path = os.path.join(repo_path, file_path)

            # 读取文件内容
            with open(full_path, 'r', encoding='utf-8') as file:
                try:
                    file_content = file.read()
                except UnicodeDecodeError:
                    print(f"Skipping file due to encoding issues: {file_path}")
                    pbar.update(1)
                    continue

            # 处理文件
            result = process_file(repo_description, file_path, file_content)

            # 将结果写入 JSONL 文件
            with open(output_file, 'a', encoding='utf-8') as f:
                json.dump(result, f)
                f.write('\n')

            # 更新进度条
            pbar.update(1)

    # 计算总体调用时间
    total_time = time.time() - start_time

    print(f"\nProcessing completed. Results saved to {output_file}")
    print(f"Total processing time: {total_time:.2f} seconds")

    return output_file, total_time


# 使用示例
if __name__ == "__main__":
    repo_path = r'D:\codegeex\api'  # 替换为您的代码仓库路径
    repo_description = """
    这是一个代码补全工具（客户端为VSCode插件）的后端api服务，可以根据用户的输入，推荐代码补全建议。
    """

    output_file, total_time = process_repository("codegeex-api",repo_path, repo_description)

    print(f"Analysis complete. Results saved to: {output_file}")
    print(f"Total time taken: {total_time:.2f} seconds")