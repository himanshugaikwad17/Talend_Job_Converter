from xml.etree import ElementTree as ET

def extract_from_internal_components(xml_path):
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
    except ET.ParseError as e:
        raise ValueError(f"Invalid XML: {str(e)}")

    nodes = []
    edges = []
    x, y_spacing = 100, 120

    components = root.findall(".//internalNodeComponents/component")

    if not components:
        raise ValueError("No <component> found under <internalNodeComponents>")

    for idx, comp in enumerate(components):
        node_id = comp.attrib.get("uniqueName")
        label = comp.attrib.get("label") or node_id
        comp_type = comp.findtext("componentType", "Component")

        if not node_id:
            continue

        nodes.append({
            "id": node_id,
            "data": {"label": comp_type},
            "position": {"x": x, "y": 100 + idx * y_spacing}
        })

        # Extract connection if present
        output = comp.find("output")
        if output is not None:
            target = output.attrib.get("link")
            if target and target.lower() != "none":
                edges.append({
                    "id": f"{node_id}-{target}",
                    "source": node_id,
                    "target": target
                })

    return nodes, edges
