__author__ = 'emmanuel'

from distutils.core import setup

setup(
    name='pdhbase',
    version='0.1.0',
    author='Livingstone S E',
    author_email='livingstone.s.e@gmail.com',
    packages=['pdhbase'],
    url='https://pypi.python.org/pypi/pdhbase',
    license='LICENSE',
    description='Read and write pandas dataframes to hbase.',
    long_description=open('README.rst').read(),
    install_requires=[
        "numpy >= 1.8.1",
        "pandas >= 0.13.0",
        "happybase >= 0.6"
    ],
)