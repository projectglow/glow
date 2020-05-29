import pytest
import render_template as rt


def test_validate_optional_arg():
    groups = {
        'test': {
            'functions': [{
                'args': [{
                    'name': 'bad',
                    'is_optional': True
                }, {
                    'name': 'ok'
                }],
                'since': '1.0',
                'doc': 'doc',
                'expr_class': 'class'
            }]
        }
    }
    with pytest.raises(AssertionError) as error:
        rt.prepare_definitions(groups)
        assert 'optional' in error.value


def test_validate_var_args():
    groups = {
        'test': {
            'functions': [{
                'args': [{
                    'name': 'bad',
                    'is_var_args': True
                }, {
                    'name': 'ok'
                }],
                'since': '1.0',
                'doc': 'doc',
                'expr_class': 'class'
            }]
        }
    }
    with pytest.raises(AssertionError) as error:
        rt.prepare_definitions(groups)
        assert 'var args' in error.value


def test_check_field_defined():
    base_func = {'name': 'function', 'doc': 'doc', 'since': '1.0', 'expr_class': 'class'}
    fields = ['name', 'doc', 'since', 'expr_class']
    for f in fields:
        new_func = base_func.copy()
        del new_func[f]
        groups = {'test': {'functions': [new_func]}}
        with pytest.raises(AssertionError) as error:
            rt.prepare_definitions(groups)
            assert f in error.value
