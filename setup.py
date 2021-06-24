# coding=utf-8
"""Install pyiqfeed into your library path."""
import setuptools
from setuptools import setup

setup(
    name='pyiqfeed',
    version='0.9.1',
    description='Handles connections to IQFeed, the market data feed by DTN',
    url='https://github.com/rhawiz/pyiqfeed',
    author='Ashwin Kapur',
    author_email='ashwin.kapur@gmail.com',
    packages=setuptools.find_packages(),
    install_requires=['numpy~=1.20.2'],
    python_requires='>=3.7',
    classifiers=[
        "Development Status :: 3 - Alpha"
    ],
)
