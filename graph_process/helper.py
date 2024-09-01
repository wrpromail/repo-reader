import re, os

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