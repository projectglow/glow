from setuptools import setup

setup(
    name='pyglow',
    version='1.0.0',
    packages=['db_genomics'],
    install_requires=[
        'pyspark==2.4.2',
        'pytest',
        'typeguard==2.5.0',
    ],
    zip_safe=False,
    author='Glow Project',
    description='Glow: Genomics on Apache Spark SQL',
    long_description=open('README.rst').read(),
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    keywords=['databricks'],
    url='https://glow-genomics.org/'
)
