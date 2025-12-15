import importlib.resources
import json
import re


def load_json(msg_type):
    # Raises FileNotFoundError if not found
    msg_json = importlib.resources.read_text(__package__, msg_type + '.json')
    data = json.loads(re.sub('^ *//.*$', '', msg_json, flags=re.MULTILINE))

    common_structs = {s['name']: s['fields'] for s in data.get('commonStructs', [])}
    if common_structs:
        _resolve_common_structs(data.get('fields', []), common_structs)
    return data


def _resolve_common_structs(fields, common_structs):
    for field in fields:
        field_type = field['type']
        struct_name = None
        if field_type.startswith('[]'):
            inner_type = field_type[2:]
            if inner_type and inner_type[0].isupper():
                struct_name = inner_type
        elif field_type and field_type[0].isupper():
            struct_name = field_type

        if struct_name and struct_name in common_structs and 'fields' not in field:
            field['fields'] = common_structs[struct_name]

        if 'fields' in field:
            _resolve_common_structs(field['fields'], common_structs)
