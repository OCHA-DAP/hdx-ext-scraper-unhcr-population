import pytest
from fields import (
    RowIterator,
    add_decoded_fields_in_iterator,
    convert_fields_in_iterator,
    convert_headers,
    encoding,
    hxltags_mapping,
    rename_fields_in_iterator,
)
from ruamel.yaml import YAML


class TestFields:
    @pytest.fixture
    def fields(self):
        return YAML().load(
            """
fields:
  field1:
    name: field1 renamed
  field2:
    name: field2 renamed
    tags: "#indicator+code"
    encoding:
      name: field2e
      tags: "#indicator+name"
      map:
        f2val1: f2val1 mapped
        """
        )["fields"]

    @pytest.fixture
    def iterator(self):
        return [
            dict(field1="f1val1", field2="f2val1", unspecified_field="X1"),
            dict(field1="f1val2", field2="f2val2", unspecified_field="X2"),
        ]

    def test_rename(self, iterator, fields):
        result = list(rename_fields_in_iterator(iterator, fields))
        assert len(result) == 2
        assert result[0]["field1 renamed"] == "f1val1"
        assert result[0]["unspecified_field"] == "X1"

    def test_encoding1(self, fields):
        encoding_map, encoding_field_names = encoding(fields)
        assert encoding_map == {"field2 renamed": {"f2val1": "f2val1 mapped"}}
        assert encoding_field_names == {"field2 renamed": "field2e"}

    def test_encoding2(self, fields):
        encoding_map, encoding_field_names = encoding(
            fields, use_original_field_names=True
        )
        assert encoding_map == {"field2": {"f2val1": "f2val1 mapped"}}
        assert encoding_field_names == {"field2": "field2e"}

    def test_hxltags_mapping1(self, fields):
        hxltags = hxltags_mapping(fields)
        assert hxltags == {
            "field1 renamed": "",
            "field2 renamed": "#indicator+code",
            "field2e": "#indicator+name",
        }

    def test_hxltags_mapping2(self, fields):
        hxltags = hxltags_mapping(fields, use_original_field_names=True)
        assert hxltags == {
            "field1": "",
            "field2": "#indicator+code",
            "field2e": "#indicator+name",
        }

    def test_add_decoded_fields_in_iterator(self, iterator, fields):
        encoding_map, encoding_field_names = encoding(
            fields, use_original_field_names=True
        )
        result = list(
            add_decoded_fields_in_iterator(iterator, encoding_map, encoding_field_names)
        )
        assert len(result) == 2
        assert result == [
            {
                "field1": "f1val1",
                "field2": "f2val1",
                "field2e": "f2val1 mapped",
                "unspecified_field": "X1",
            },
            {
                "field1": "f1val2",
                "field2": "f2val2",
                "field2e": None,
                "unspecified_field": "X2",
            },
        ]

    def test_convert_fields_in_iterator(self, iterator, fields):
        result = list(convert_fields_in_iterator(iterator, fields))
        assert len(result) == 2
        assert result == [
            {
                "field1 renamed": "f1val1",
                "field2 renamed": "f2val1",
                "field2e": "f2val1 mapped",
                "unspecified_field": "X1",
            },
            {
                "field1 renamed": "f1val2",
                "field2 renamed": "f2val2",
                "field2e": None,
                "unspecified_field": "X2",
            },
        ]

    def test_convert_headers(self, fields):
        new_headers = convert_headers(["field1", "field2", "unspecified_field"], fields)
        assert new_headers == [
            "field1 renamed",
            "field2 renamed",
            "field2e",
            "unspecified_field",
        ]

    def test_row_iterator_with_fields(self, iterator, fields):
        rowit = RowIterator(
            ["field1", "field2", "unspecified_field"], iterator
        ).with_fields(fields)
        assert rowit.headers() == [
            "field1 renamed",
            "field2 renamed",
            "field2e",
            "unspecified_field",
        ]
        assert rowit.hxltags_mapping() == {
            "field1 renamed": "",
            "field2 renamed": "#indicator+code",
            "field2e": "#indicator+name",
        }
        result = list(rowit)
        assert len(result) == 2
        assert result == [
            {
                "field1 renamed": "f1val1",
                "field2 renamed": "f2val1",
                "field2e": "f2val1 mapped",
                "unspecified_field": "X1",
            },
            {
                "field1 renamed": "f1val2",
                "field2 renamed": "f2val2",
                "field2e": None,
                "unspecified_field": "X2",
            },
        ]

    def test_row_iterator_with_sum_field(self):
        data = [dict(a=1, b=10), dict(a=2, b=20)]
        rowit = RowIterator(["a", "b"], data).with_sum_field("c", sum_fields=["a", "b"])
        assert rowit.headers() == ["a", "b", "c"]
        assert list(rowit) == [dict(a=1, b=10, c=11), dict(a=2, b=20, c=22)]
