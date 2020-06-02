from setuptools import setup, setuptools
import imp

version = imp.load_source('version', 'version.py').VERSION

setup(name='glow.py',
      version=version,
      packages=setuptools.find_packages(),
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
      url='https://projectglow.io')
