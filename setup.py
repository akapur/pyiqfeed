# coding=utf-8
"""Install pyiqfeed into your library path."""
import setuptools
from setuptools import setup

setup(
    name='pyiqfeed',
    version='0.9',
    description='Handles connections to IQFeed, the market data feed by DTN',
    url='https://github.com/akapur/pyiqfeed',
    author='Ashwin Kapur',
    author_email='ashwin.kapur@gmail.com',
    license='GPL v2',
    packages=setuptools.find_packages(),
    install_requires=['numpy~=1.20.2'],

)
