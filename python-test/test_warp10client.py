import pytest

from warp10 import Warp10Client

warp10_client = Warp10Client({'host': '', 'port': 0, 'read_token': '', 'write_token': ''})  # Just need a dummy instance

def _schema(value_type):
    return {
            'columns':
            [
                {'name': 'timestamp', 'type': 'bigint'},
                {'name': 'latitude', 'type': 'double'},
                {'name': 'longitude', 'type': 'double'},
                {'name': 'elevation', 'type': 'bigint'},
                {'name': 'identifier', 'type': 'string'},
                {'name': 'value', 'type': value_type},
                {'name': 'attributes', 'type': 'string'}
            ]
        }

def _basic_schema():
    return {
            'columns':
            [
                {'name': 'timestamp', 'type': 'bigint'},
                {'name': 'identifier', 'type': 'string'},
                {'name': 'value', 'type': 'bigint'}
            ]
        }

def test_convert_row_to_gts_string():
    row = [100, 24.67, 12.563, 10000, "time{k=v,x=y}", "12345", "{}"]
    assert warp10_client._convert_row_to_gts_string(_schema('bigint'), row) == '100/24.67:12.563/10000 time{k=v,x=y} 12345'

def test_convert_row_to_gts_string_with_string_value():
    row = [100, 24.67, 12.563, 10000, "time{k=v,x=y}", "12345", "{}"]
    assert warp10_client._convert_row_to_gts_string(_schema('string'), row) == '100/24.67:12.563/10000 time{k=v,x=y} "12345"'

def test_convert_row_to_gts_string_with_no_geo():
    row = [100, "time{k=v,x=y}", "12345", "{}"]
    assert warp10_client._convert_row_to_gts_string(_basic_schema(), row) == '100// time{k=v,x=y} 12345'

def test_convert_row_to_gts_string_with_no_labels():
    row = [100, "time", "12345", "{}"]
    assert warp10_client._convert_row_to_gts_string(_basic_schema(), row) == '100// time{} 12345'

def _assert_missing_column_error(schema, error_text):
    row = [100, "time{}", "12345", "{}"]
    with pytest.raises(TypeError) as exception_info:
        warp10_client._convert_row_to_gts_string(schema, row)
    assert error_text in str(exception_info.value)

def test_convert_row_to_gts_string_error_no_identifier():
    schema = {
            'columns':
            [
                {'name': 'timestamp', 'type': 'bigint'},
                {'name': 'value', 'type': 'bigint'}
            ]
        }

    _assert_missing_column_error(schema, 'Row is missing an identifier')

def test_convert_row_to_gts_string_error_no_timestamp():
    schema = {
            'columns':
            [
                {'name': 'identifier', 'type': 'string'},
                {'name': 'value', 'type': 'bigint'}
            ]
        }

    _assert_missing_column_error(schema, 'Row is missing a timestamp')

def test_convert_row_to_gts_string_error_no_value():
    schema = {
            'columns':
            [
                {'name': 'timestamp', 'type': 'bigint'},
                {'name': 'identifier', 'type': 'string'}
            ]
        }

    _assert_missing_column_error(schema, 'Row is missing a value')
