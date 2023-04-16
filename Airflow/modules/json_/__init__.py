def save_json(file_name, json_data):
    import json

    with open(f"./json_file/{file_name}.json", "w") as f:
        json.dump(json_data, f)


def load_json(file_name):
    import json

    with open(f"./json_file/{file_name}.json", "r") as f:
        return json.load(f)