import os
import re

extensions = (
    # 常见编程语言
    '.py',    # Python
    '.java',  # Java
    '.class', # Java 编译文件
    '.cpp',   # C++
    '.c',     # C
    '.h',     # C/C++ 头文件
    '.hpp',   # C++ 头文件
    '.cs',    # C#
    '.js',    # JavaScript
    '.ts',    # TypeScript
    '.go',    # Go
    '.rb',    # Ruby
    '.php',   # PHP
    '.swift', # Swift
    '.kt',    # Kotlin
    '.rs',    # Rust
    '.scala', # Scala
)

def get_code_files(repo_path, ext=extensions):
    code_files = []
    for root, dirs, files in os.walk(repo_path):
        for file in files:
            if file.endswith(ext):
                # 计算相对路径
                relative_path = os.path.relpath(os.path.join(root, file), repo_path)
                code_files.append(relative_path)
    return code_files




def get_project_name(input_string, custom_name=None):
    if custom_name:
        return custom_name

    # 处理本地路径
    if os.path.sep in input_string:
        return os.path.basename(input_string.rstrip(os.path.sep))

    # 处理Git SSH地址
    ssh_match = re.match(r'git@.*:(.+)\.git', input_string)
    if ssh_match:
        return ssh_match.group(1).split('/')[-1]

    # 处理Git HTTP地址
    http_match = re.match(r'https?://.*?/(.+?)(?:\.git)?$', input_string)
    if http_match:
        return http_match.group(1).split('/')[-1]

    # 如果无法识别，返回原字符串
    return input_string


