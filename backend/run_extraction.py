import sys
import json
from extract_graph import extract_talend_graph

html_file = sys.argv[1]
nodes, edges = extract_talend_graph(html_file)

with open("../public/nodes.json", "w") as f:
    json.dump(nodes, f, indent=2)

with open("../public/edges.json", "w") as f:
    json.dump(edges, f, indent=2)

print("✅ Extraction completed. JSON files saved in /public/")
