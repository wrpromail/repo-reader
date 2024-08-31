import os
from git import Repo


def get_recent_commits(repo_path, num_commits=5):
    try:
        # 打开仓库
        repo = Repo(repo_path)

        # 获取当前分支
        current_branch = repo.active_branch

        # 获取最近的提交
        commits = list(repo.iter_commits(current_branch, max_count=num_commits))

        commit_info = []
        for commit in commits:
            # 获取修改的文件
            modified_files = [item.a_path for item in commit.diff(commit.parents[0])]

            # 构建提交信息字典
            commit_data = {
                'hash': commit.hexsha,
                'author': f"{commit.author.name} <{commit.author.email}>",
                'date': commit.committed_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                'message': commit.message.strip(),
                'modified_files': modified_files
            }
            commit_info.append(commit_data)

        return commit_info

    except Exception as e:
        print(f"Error: {str(e)}")
        return None


# 使用示例
if __name__ == "__main__":
    repo_path = "/Users/wangrui/zhipu/eduplatform-backend"
    commits = get_recent_commits(repo_path, 5)

    if commits:
        for commit in commits:

            print(f"Commit: {commit['hash']}")
            print(f"Author: {commit['author']}")
            print(f"Date: {commit['date']}")
            print(f"Message: {commit['message']}")
            print("Modified files:")
            for file in commit['modified_files']:
                print(f"  - {file}")
            print("---")