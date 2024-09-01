# 1. query_by_filename
match_dict = {"pattern": "(f:File)"}
where_dict = {"condition": "f.name = $filename", "params": {"filename": "example.txt"}}
return_dict = {"f": "f"}

# 2. query_by_extension
match_dict = {"pattern": "(f:File)"}
where_dict = {"condition": "f.extension = $extension", "params": {"extension": "py"}}
return_dict = {"f": "f"}

# 3. query_by_directory_name
match_dict = {"pattern": "(d:Directory)"}
where_dict = {"condition": "d.name = $directory_name", "params": {"directory_name": "src"}}
return_dict = {"d": "d"}

# 4. query_files_in_directory
match_dict = {"pattern": "(d:Directory)-[:CONTAINS]->(f:File)"}
where_dict = {"condition": "d.name = $directory_name", "params": {"directory_name": "src"}}
return_dict = {"f": "f"}

# 5. query_files_by_path_substring
match_dict = {"pattern": "(f:File)"}
where_dict = {"condition": "f.relative_path CONTAINS $substring", "params": {"substring": "/lib/"}}
return_dict = {"f": "f"}

# 示例调用（以 query_by_filename 为例）
# query_file(graph, match_dict, where_dict, return_dict)