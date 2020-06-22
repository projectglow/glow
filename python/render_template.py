#!/usr/bin/env python

# Copyright 2019 The Glow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A script that turns a YAML file in the format of functions.yml into language specific clients
# using jinja2 templates.

import argparse
import jinja2
import yaml

FUNCTIONS_YAML = './functions.yml'
SCALA_TYPES = {
    'str': 'String',
    'int': 'Int',
    'double': 'Double',
    'lambda1': 'Column => Column',
    'lambda2': '(Column, Column) => Column'
}
PYTHON_TYPES = {
    'int': 'int',
    'str': 'str',
    'double': 'float',
    'lambda1': 'Column',
    'lambda2': 'Column'
}


def wrap(value, before, after):
    return before + str(value) + after


def scala_type(value):
    if not 'type' in value:
        return 'Column'
    return SCALA_TYPES[value['type']]


def python_type(value):
    if not 'type' in value:
        return 'Union[Column, str]'
    return PYTHON_TYPES[value['type']]


def fmt_scala_signature(value):
    if value.get('is_var_args'):
        return f'{value["name"]}: {scala_type(value)}*'
    return value['name'] + ': ' + scala_type(value)


def fmt_scala_call(value):
    if value.get('is_var_args') and not 'type' in value:
        return f'{value["name"]}.map(_.expr)'
    if value.get('is_var_args'):
        return f'{value["name"]}.map(Literal(_))'
    if not 'type' in value:  # no type means column
        return value['name'] + '.expr'
    if value['type'] in ['lambda1', 'lambda2']:
        return f'createLambda({value["name"]})'
    return f"Literal({value['name']})"


def fmt_python_signature(value):
    if value.get('is_var_args'):
        return f'*{value["name"]}: {python_type(value)}'
    default = ' = None' if value.get('is_optional') else ''
    return f'{value["name"]}: {python_type(value)}{default}'


def fmt_python_call(value):
    if value.get('is_var_args'):
        # Convert column inputs to their java equivalent, otherwise rely on built in conversions
        # between Python and Scala
        converter = ', _to_java_column' if not 'type' in value else ''
        return f'_to_seq(sc(), {value["name"]}{converter})'
    if not 'type' in value or value['type'] in ['lambda1', 'lambda2']:
        return f'_to_java_column({value["name"]})'
    return value["name"]


def prepare_definitions(groups):
    '''
    Prepare the function definitions from the YAML file into definitions that can be rendered into
    templates. Validation should also occur in this function.
    
    Currently, this function only performs validation. Looking forward, it can be used for
    preprocessing to handle features like multiple optional arguments and overloaded function
    definitions.
    '''

    required_fields = ['name', 'doc', 'since', 'expr_class']
    for group in groups.values():
        for function in group['functions']:
            for field in required_fields:
                check_field_defined(function, field)
            for i, arg in enumerate(function['args']):
                if arg.get('is_optional'):
                    assert i == len(function['args']) - 1, f'Only the last argument in the argument'
                    'list can be optional ({arg})'
                if arg.get('is_var_args'):
                    assert i == len(function['args']) - 1, f'Only the last argument in the argument'
                    'list can be var args'
    return groups


def check_field_defined(value, field):
    assert value.get(field), f'Must provide "{field}" field'


def render_template(template_path, output_path, **kwargs):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader('/'),
                             trim_blocks=True,
                             lstrip_blocks=True)

    env.filters['wrap'] = wrap
    env.filters['fmt_scala_signature'] = fmt_scala_signature
    env.filters['fmt_scala_call'] = fmt_scala_call
    env.filters['fmt_python_signature'] = fmt_python_signature
    env.filters['fmt_python_call'] = fmt_python_call

    template = env.get_template(template_path)
    if output_path is None:
        print(template.render(kwargs))
    else:
        template.stream(kwargs).dump(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Render function template')
    parser.add_argument('template_path', help='Path to input template')
    parser.add_argument(
        'output_path',
        nargs='?',
        help='Where to put rendered template. If not provided, template will be rendered to stdout')
    args = parser.parse_args()

    function_groups = yaml.load(open(FUNCTIONS_YAML), Loader=yaml.SafeLoader)
    groups_to_render = prepare_definitions(function_groups)
    render_template(args.template_path, args.output_path, groups=groups_to_render)
