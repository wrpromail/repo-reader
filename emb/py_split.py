import ast
import astor


class CodeSplitter(ast.NodeVisitor):
    def __init__(self):
        self.entities = []

    def visit_ClassDef(self, node):
        self.add_entity(node, 'class')

    def visit_FunctionDef(self, node):
        if isinstance(node.parent, ast.Module):
            self.add_entity(node, 'function')

    def add_entity(self, node, entity_type):
        entity_code = astor.to_source(node)
        self.entities.append({
            'name': node.name,
            'type': entity_type,
            'code': entity_code,
            'start_line': node.lineno,
            'end_line': node.end_lineno
        })


def split_code(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        code = file.read()

    tree = ast.parse(code)
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node

    splitter = CodeSplitter()
    splitter.visit(tree)

    return splitter.entities


if __name__ == "__main__":
    # 使用示例
    file_path = '/Users/wangrui/zhipu/eduplatform-backend/apps/edu/curd/mysql_curd.py'
    entities = split_code(file_path)

    for cls in entities:
        print(f"Entity: {cls['name']}, Lines: {cls['start_line']}-{cls['end_line']}")
        print(cls['type'])
        print("---")
