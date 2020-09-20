"""
Utilities for using a "fields" structure to define hxl tags, rename fields and decode fields values.
The fields structure looks is a dictionary that looks like this:
  field1:
    name: 'New name for field1'
    tags: '#meta+tags+for+field1'
  field2:
    name: "New name for field2"
    tags: '#indicator+code'
    encoding:
      name: "Field2 names"    # Name of the new field created out of field2 by applying the map
      tags: '#indicator+name' # HXL tags for the new field
      map:
        f2val1: "field2 value 1 mapped"
        f2val2: "field2 value 2 mapped"

Use convert_fields_in_iterator to convert an iterator, hxltags_mapping to extract mapping of field names (new or old)
and finally convert_headers to convert the headers.
"""


def rename_fields_in_iterator(iterator, fields):
    """Rename fields in iterator.
    Function expects an iterable of dictionaries and field description.
    Field description is a dictionary where key is the original field name and value is a dictionary optionally
    containing a "name" key with the value containing the new name for the field.
    """
    for row in iterator:
        yield {
            fields.get(key, {}).get("name", key): value for key, value in row.items()
        }


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


def convert_headers(headers, fields):
    "Rename and eventually add new fields into headers using the fields structure"
    _, encoding_field_names = encoding(fields, use_original_field_names=True)

    new_headers = []
    for field in headers:
        new_headers.append(fields.get(field, {}).get("name", field))
        if field in encoding_field_names:
            new_headers.append(encoding_field_names[field])

    return new_headers


class RowIteratorMixin(object):
    """Mixin defining RowIterator builder interface"""
    def headers(self):
        "List of field names of the row iterator"
        return self._headers

    def hxltags_mapping(self):
        "Dictionary mapping field names to hxl tags"
        return {}
    
    def with_sum_field(self, field_name, hxltag="", sum_fields=[]):
        "Create a new column fith *field_name* and *hxltag* that is a sum of *sum_fields*"
        return RowIteratorWithSumField(self, field_name, hxltag, sum_fields)

    def with_fields(self, fields):
        """Use fields structure to perform the conversion."""
        return RowIteratorWithFields(self, fields)

    def sort_by(self, field, descending=False):
        headers=self.headers()
        mapping=self.hxltags_mapping()
        data = sorted(self, key=lambda x,f=field:x.get(f), reverse=descending)
        return ListIterator(data, headers=headers, hxltags_mapping=mapping)

    def to_list_iterator(self):
        headers=self.headers()
        mapping=self.hxltags_mapping()
        data = list(self)
        return ListIterator(data, headers=headers, hxltags_mapping=mapping)

    def select(self, condition):
        headers=self.headers()
        mapping=self.hxltags_mapping()
        data = [row for row in self if condition(row)]
        return ListIterator(data, headers=headers, hxltags_mapping=mapping)


    def to_csv(self, f, sep=","):
        "Write row iterator to a file *f*, which can be a file object or a string with path."
        def cell(x):
            if type(x) is str:
                return f'"{repr(x)[1:-1]}"'
            try:
                float(x)
                return str(x)
            except:
                return repr(x)

        if type(f) is str:
            f=open(f,"w")
        headers = self.headers()
        f.write(sep.join(headers)+"\n")
        mapping = self.hxltags_mapping()
        f.write(sep.join(mapping.get(x,"") for x in headers)+"\n")
        for row in self:
            f.write(sep.join(cell(row.get(x,"")) for x in headers)+"\n")
        f.close()


    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iterator)

class RowIteratorProxyMixin(RowIteratorMixin):
    """Mixin defining RowIterator builder interface"""
    def headers(self):
        "List of field names of the row iterator"
        return self.rowit.headers()

    def hxltags_mapping(self):
        "Dictionary mapping field names to hxl tags"
        return self.rowit.hxltags_mapping()

    def reset(self):
        self.rowit.reset()
        return self

    def __next__(self):
        return next(self.rowit)


class RowIterator(RowIteratorMixin):
    def __init__(self, headers, iterator):
        self._headers = headers
        self._iterator = iter(iterator)
    def reset(self):
        raise Exception("Can't reset RowIterator based on iterator")

class ListIterator(RowIteratorMixin):
    def __init__(self, data, headers=None, hxltags_mapping=None):
        self._headers = headers or []
        self._data = data
        self._hxltags_mapping = hxltags_mapping or {}
        self.reset()

    def hxltags_mapping(self):
        "Dictionary mapping field names to hxl tags"
        return self._hxltags_mapping

    def reset(self):
        self._iterator = iter(self._data)
        return self

    def column(self, field):
        return [row.get(field) for row in self._data]

    def auto_headers(self, scan_all_rows=True):
        """Automatically add all fields in data.
        By default all rows are scanned *scan_all_rows* is False,
        in which case only the first row is used. 
        """
        extra_headers=set([])
        data = self._data if scan_all_rows else self._data[:1]
        for row in data:
            for field in row.keys():
                if field not in self._headers:
                    extra_headers.add(field)

        self._headers += sorted(extra_headers)
        return self

class RowIteratorWithFields(RowIteratorMixin):
    """Row iterator doing the field conversion"""
    def __init__(self, rowit, fields):
        self.rowit = rowit
        self._iterator = convert_fields_in_iterator(rowit, fields)
        self._fields = fields

    def reset(self):
        self.rowit.reset()
        self._iterator = convert_fields_in_iterator(self.rowit, self._fields)
        return self

    def headers(self):
        "List of field names of the row iterator"
        return convert_headers(self.rowit.headers(), self._fields)

    def hxltags_mapping(self):
        mapping = self.rowit.hxltags_mapping()
        mapping.update(hxltags_mapping(self._fields))
        return mapping


class RowIteratorWithSumField(RowIteratorProxyMixin):
    def __init__(self, rowit, field_name, hxltag, sum_fields):
        self.rowit = rowit
        self.field_name = field_name
        self.hxltag = hxltag
        self.sum_fields = sum_fields

    def headers(self):
        "List of field names of the row iterator"
        headers = self.rowit.headers()[:]
        if self.field_name not in headers:
            headers.append(self.field_name)
        return headers

    def hxltags_mapping(self):
        mapping = self.rowit.hxltags_mapping()
        mapping[self.field_name] = self.hxltag
        return mapping

    def __next__(self):
        row = next(self.rowit)
        value = 0.0
        for field in self.sum_fields:
            try:
                value += float(row.get(field, 0))
            except ValueError:
                pass
        if value==int(value):
            value=int(value)
        row[self.field_name] = value
        return row
