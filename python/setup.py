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

from setuptools import setup, setuptools
import imp
from pathlib import Path


def relative_file(path):
    return (Path(__file__).parent / path).as_posix()


version = imp.load_source('version', relative_file('version.py')).VERSION

setup(name='glow.py',
      version=version,
      packages=setuptools.find_packages(),
      install_requires=[
          'nptyping>=2.5.0',
          'numpy>=1.23.5',
          'opt_einsum>=3.2.0',
          'pandas>=1.5.3',
          'scipy>=1.10.0',
          'statsmodels>=0.13.5',
          'typeguard>=2.13.3',
      ],
      author='The Glow Authors',
      description='An open-source toolkit for large-scale genomic analysis',
      long_description=open(relative_file('README.rst')).read(),
      long_description_content_type='text/x-rst',
      license='Apache License 2.0',
      classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3.10',
      ],
      url='https://projectglow.io')
