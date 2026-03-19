import pytest

from app.utils.schema_file_parser import SchemaFileParser


def test_parse_filters_only_valid_schema_paths():
    """
    Test the parser correctly filters valid schema paths
    """
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
        ("Schemas/a/b.json", False),          # wrong case in prefix
        ("other/schemas/a/b.json", False),    # doesn't start with schemas/
    ],
)
def test_filter_regex_expectations(path: str, expected: bool):
    """
    Test the parser correctly filters valid schema paths
    """
    parser = SchemaFileParser()
    filtered = parser.filter_files([path])
    assert (filtered == [path]) is expected


def test_parse_empty_or_whitespace_only_returns_empty_list():
    parser = SchemaFileParser()

    assert parser.parse("") == []
    assert parser.parse("   \n\t  ") == []
