from setuptools import setup

setup(
    name='glow-py',
    version='0.1.0',
    packages=['glow'],
    install_requires=[
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
    url='https://projectglow.io'
)
