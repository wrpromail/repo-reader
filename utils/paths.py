import os

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

# 示例用法
repo_path = 'D:\eduplatform-backend'  # 替换为你的代码仓库路径
  # 可以根据需要添加扩展名
code_files = get_code_files(repo_path, extensions)

