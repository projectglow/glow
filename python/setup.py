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

version = imp.load_source('version', 'version.py').VERSION

setup(name='glow.py',
      version=version,
      packages=setuptools.find_packages(),
      install_requires=[
          'nptyping==1.1.0',
          'numpy>=1.17.4',
          'pandas>=0.25.3',
          'typeguard==2.5.0',
      ],
      author='The Glow Authors',
      description='An open-source toolkit for large-scale genomic analysis',
      long_description=open('README.rst').read(),
      long_description_content_type='text/x-rst',
      license='Apache License 2.0',
      classifiers=[
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3.7',
      ],
      url='https://projectglow.io')
