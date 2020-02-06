import pytest
import render_template as rt

def test_validate_optional_arg():
    groups = { 'test': { 'functions': [ { 'args': [ { 'name': 'bad', 'is_optional': True }, { 'name':
        'ok' } ] } ] } }
    with pytest.raises(AssertionError) as error:
        rt.prepare_definitions(groups)
        assert 'optional' in error.value

def test_validate_var_args():
    groups = { 'test': { 'functions': [ { 'args': [ { 'name': 'bad', 'is_var_args': True }, { 'name':
        'ok' } ] } ] } }
    with pytest.raises(AssertionError) as error:
        rt.prepare_definitions(groups)
        assert 'var args' in error.value
