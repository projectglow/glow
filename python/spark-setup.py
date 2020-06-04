# Minimal setup.py for PySpark

from setuptools import setup

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
