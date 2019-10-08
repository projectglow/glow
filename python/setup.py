from setuptools import setup

setup(
    name='glowpy',
    version='1.0.3',
    packages=['db_genomics'],
    # install_requires=[
    #     'typeguard==2.5.0',
    # ],
    author='Glow Project',
    description='Glow: Genomics on Apache Spark',
    long_description=open('../README.rst').read(),
    long_description_content_type='text/x-rst',
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
    ],
    url='https://github.com/projectglow/glow'
)
