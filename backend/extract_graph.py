import xml.etree.ElementTree as ET
from difflib import get_close_matches

def extract_generic_and_safe(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    nodes = []
    real_edges = []
    used_ids = set()
    unique_name_to_uid = {}
    component_registry = {}
    x, y_spacing = 120, 120

    def unique_id(base):
        counter = 1
        while f"{base}_{counter}" in used_ids:
            counter += 1
        new_id = f"{base}_{counter}"
        used_ids.add(new_id)
        return new_id

    print("\n=== 🔍 Parsing Components ===")
    for idx, comp in enumerate(root.findall(".//node")):
        comp_type = comp.attrib.get("componentName", "UNKNOWN").strip()
        label = comp.attrib.get("label", "").strip()
        name = comp.attrib.get("name", "").strip()
        is_activate = comp.attrib.get("isActivate", "true").strip().lower()

        # Extract UNIQUE_NAME
        unique_name = None
        for param in comp.findall("elementParameter"):
            if param.attrib.get("name") == "UNIQUE_NAME":
                unique_name = param.attrib.get("value")
                break

        display_label = label or name or comp_type or "Unknown"
        base_id = f"{comp_type}_{display_label}".replace(" ", "_")
        uid = unique_id(base_id)

        if unique_name:
            unique_name_to_uid[unique_name] = uid

        # SQL Query detection
        params = {p.attrib.get("name"): p.attrib.get("value") for p in comp.findall("elementParameter")}
        query = params.get("QUERY") or params.get("Sql Query")
        if query and query.startswith('"') and query.endswith('"'):
            query = query[1:-1]

        component_registry[uid] = {
            "id": uid,
            "unique_name": unique_name,
            "label": display_label,
            "component_type": comp_type,
            "is_activate": is_activate,
            "sql": query,
            "position": {"x": x, "y": 100 + idx * y_spacing}
        }

    source_ids, target_ids = [], []

    print("\n=== 🔗 Resolving Connections Using UNIQUE_NAME ===")
    for conn in root.findall(".//connection"):
        src = conn.attrib.get("source")
        tgt = conn.attrib.get("target")
        label = conn.attrib.get("connectorName", "SUBJOB_OK")

        src_uid = unique_name_to_uid.get(src)
        tgt_uid = unique_name_to_uid.get(tgt)

        print(f"🔍 Connection: {src} → {tgt} → {src_uid} → {tgt_uid}")

        if src_uid and tgt_uid:
            real_edges.append({
                "id": f"{src_uid}-{tgt_uid}",
                "source": src_uid,
                "target": tgt_uid,
                "label": label
            })
            source_ids.append(src_uid)
            target_ids.append(tgt_uid)

    print("\n=== ✅ Classifying Component Status ===")
    nodes = []
    for uid, comp in component_registry.items():
        if comp["is_activate"] == "false":
            status = "deactivated"
        elif uid in source_ids or uid in target_ids:
            status = "active"
        else:
            status = "inactive"

        print(f"  ⬛ {comp['label']} ({comp['unique_name']}): {status}")

        nodes.append({
            "id": uid,
            "data": {
                "label": comp["label"],
                "status": status,
                "sql": comp["sql"]
            },
            "position": comp["position"]
        })

    return nodes, real_edges, real_edges
