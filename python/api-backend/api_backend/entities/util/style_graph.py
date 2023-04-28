def style_graph(data, template):
    encoded_data = add_graph_encodings(data)
    node_data = [{**x, "index": index} for index, x in enumerate(encoded_data["nodes"])]

    link_data = [
        {
            "source": next((i for i, a in enumerate(node_data) if a["id"] == x["source"]), None),
            "target": next((i for i, a in enumerate(node_data) if a["id"] == x["target"]), None),
        }
        for x in encoded_data["edges"]
    ]

    data_spec = {
        "node-data": node_data,
        "link-data": link_data,
    }
    return merge_data_to_template(template, data_spec)


def merge_data_to_template(template, data_spec):
    node_data = data_spec["node-data"]
    link_data = data_spec["link-data"]
    data = template["data"]

    for d in data:
        if d["name"] == "node-data":
            d["values"] = node_data
        elif d["name"] == "link-data":
            d["values"] = link_data
    template["data"] = data
    return template


def add_graph_encodings(data):
    colors = get_categorical_colors(data)
    warn = "#d13438"
    normal = "#fdfeff"
    bold = "#31302e"

    return {
        "nodes": [
            {
                **node,
                "encoding": {
                    "shape": "diamond" if node["relationship"] == "target" else "circle",
                    "fill": colors.get(node["type"]),
                    "stroke": bold if node["relationship"] == "target" else warn if node["flag"] == 1 else normal,
                    "strokeWidth": 3 if node["flag"] == 1 or node["relationship"] == "target" else 1,
                    "opacity": 1.0,
                    "size": 25 if node["relationship"] == "target" else 20 if node["relationship"] == "related" else 10,
                },
            }
            for node in data["nodes"]
        ],
        "edges": list({f"{edge['source']}-{edge['target']}": edge for edge in data["edges"]}.values()),
    }


def nominal(index):
    colors = [
        "#32bb51",
        "#f87f7f",
        "#37b4b4",
        "#baa332",
        "#f66dec",
        "#39b0d2",
        "#90af32",
        "#f778b7",
        "#35b796",
        "#e29132",
    ]
    return colors[index % 10]


def get_categorical_colors(data):
    types = set(node["type"] for node in data.get("nodes", []))
    colors = {"entity": "#787672", "EntityID": "#787672"}
    for index, category in enumerate(types):
        if category not in colors:
            colors[category] = nominal(index)
    return colors
