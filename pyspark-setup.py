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


# Minimal setup.py for PySpark for Python and docs testing
from setuptools import setup
import sys

try:
    exec(open('pyspark/version.py').read())
except IOError:
    print("Failed to load PySpark version file for packaging. You must be in Spark's python dir.",
          file=sys.stderr)
    sys.exit(-1)
VERSION = __version__  # noqa

setup(name='pyspark',
      version=VERSION,
      packages=['pyspark',
                'pyspark.mllib',
                'pyspark.mllib.linalg',
                'pyspark.mllib.stat',
                'pyspark.ml',
                'pyspark.ml.linalg',
                'pyspark.ml.param',
                'pyspark.sql',
                'pyspark.sql.avro',
                'pyspark.sql.pandas',
                'pyspark.streaming'],
      install_requires=['py4j==0.10.9'])
