import json

def dict_to_jsonl(data):
    """
    Convert a list of dictionaries (or a single dictionary) to a jsonl string.

    Args:
    data: A list of dictionaries or a single dictionary.

    Returns:
    str: A string where each line is a JSON object.
    """
    if isinstance(data, dict):
        data = [data]
    return '\n'.join(json.dumps(item) for item in data)