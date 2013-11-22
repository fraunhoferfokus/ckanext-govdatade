import json


def normalize_extras(source):
    if type(source) == dict:
        return {key: normalize_extras(value) for key, value in source.items()}
    elif type(source) == list:
        return [normalize_extras(item) for item in source]
    elif isinstance(source, basestring) and is_valid(source):
        return normalize_extras(json.loads(source))
    else:
        return source


def is_valid(source):
    try:
        value = json.loads(source)
        return type(value) == dict or type(value) == list
    except ValueError:
        return False
