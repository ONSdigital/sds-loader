import pytest

from app.utils.schema_file_parser import SchemaFileParser


def test_parse_filters_only_valid_schema_paths():
    parser = SchemaFileParser()

    message = "\n".join(
        [
            "schemas/a/schema.json",          # valid
            "schemas/b/other.json",           # valid
            "schemas/a/nested/file.json",     # invalid (too deep)
            "schemas/a/schema.JSON",          # invalid (case-sensitive extension)
            "schemas/a/schema.txt",           # invalid (wrong extension)
            "schema/a/schema.json",           # invalid (wrong prefix)
            "/schemas/a/schema.json",         # invalid (absolute path)
            "schemas/a/",                     # invalid (missing filename)
            "schemas//schema.json",           # invalid (missing folder segment)
            "schemas/a/schema.json.bak",      # invalid (extra suffix)
        ]
    )

    assert parser.parse(message) == [
        "schemas/a/schema.json",
        "schemas/b/other.json",
    ]


def test_parse_splits_on_any_whitespace_not_just_newlines():
    parser = SchemaFileParser()

    # message.split() splits on spaces, newlines, tabs, repeated whitespace, etc.
    message = "schemas/a/one.json  schemas/b/two.json\t\nschemas/c/three.json"

    assert parser.parse(message) == [
        "schemas/a/one.json",
        "schemas/b/two.json",
        "schemas/c/three.json",
    ]


@pytest.mark.parametrize(
    "path,expected",
    [
        ("schemas/a/b.json", True),
        ("schemas/abc/def.json", True),
        ("schemas/a/b.JSON", False),          # case-sensitive
        ("schemas/a/b", False),               # no extension
        ("schemas/a/b.json/extra", False),    # too deep
        ("schemas/a//b.json", False),         # empty segment => doesn't match [^/]+
        ("schemas//b.json", False),           # empty segment
        ("schemas/a/", False),                # missing filename
        ("schemas/a/b.json ", False),         # trailing space would be stripped by split(), but as a raw path it wouldn't match
        ("Schemas/a/b.json", False),          # wrong case in prefix
        ("other/schemas/a/b.json", False),    # doesn't start with schemas/
    ],
)
def test_filter_regex_expectations(path, expected):
    parser = SchemaFileParser()
    filtered = parser._filter_files([path])
    assert (filtered == [path]) is expected


def test_parse_empty_or_whitespace_only_returns_empty_list():
    parser = SchemaFileParser()

    assert parser.parse("") == []
    assert parser.parse("   \n\t  ") == []
