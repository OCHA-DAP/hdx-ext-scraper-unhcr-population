def rename_fields_in_iterator(iterator, fields):
    """Rename fields in iterator.
    Function expects an iterable of dictionaries and field description.
    Field description is a dictionary where key is the original field name and value is a dictionary optionally
    containing a "name" key with the value containing the new name for the field.
    """
    for row in iterator:
        yield {fields.get(key, {}).get("name", key): value for key, value in row.items()}


def encoding(fields, use_original_field_names=False):
    """Convert fields structure to encoding maps for values and field names.
    Expects a fields dictionary structure, returns a tuple of two dictionaries where the keys are the field names
    (either original or new depending on the use_original_field_names parameter).
    Resulting dictionaries map the values for each field and field names. 
    """
    encoding_map = {}
    encoding_field_names = {}
    for original_field_name, f in fields.items():
        if "encoding" in f:
            if f["encoding"].get("expand", True):
                if use_original_field_names:
                    key = original_field_name
                else:
                    key = f.get("name", original_field_name)

                encoding_field_names[key] = f["encoding"].get("name", f"{key}_")
                encoding_map[key] = f["encoding"].get("map", {})
    return encoding_map, encoding_field_names


def hxltags_mapping(fields, use_original_field_names=False):
    """Convert fields structure to a map from field names to hxl tags.
    """
    _, encoding_field_names = encoding(
        fields, use_original_field_names=use_original_field_names
    )
    hxltags = {}
    for original_field_name, f in fields.items():
        if use_original_field_names:
            key = original_field_name
        else:
            key = f.get("name", original_field_name)
        hxltags[key] = f.get("tags", "")

        if "encoding" in f:
            if f["encoding"].get("expand", True):
                hxltags[encoding_field_names[key]] = f["encoding"].get("tags", f"")
    return hxltags


def add_decoded_fields_in_iterator(iterator, encoding_map, encoding_field_names):
    """Add decoded fields in an iterator"""
    for row in iterator:
        added_fields = {
            encoding_field_names[key]: encoding_map[key].get(value)
            for key, value in row.items()
            if key in encoding_map
        }
        row.update(added_fields)
        yield row


def convert_fields_in_iterator(iterator, fields):
    """Rename field names and eventually add fields with decoded values as defined in the fields structure.
    """
    encoding_map, encoding_field_names = encoding(fields)
    for x in add_decoded_fields_in_iterator(
        rename_fields_in_iterator(iterator, fields), encoding_map, encoding_field_names
    ):
        yield x
