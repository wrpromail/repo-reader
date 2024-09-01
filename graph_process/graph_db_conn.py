from py2neo import Graph

uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"
graph = Graph(uri, auth=(user, password))
