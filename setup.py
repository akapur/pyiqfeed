# coding=utf-8
"""Install pyiqfeed into your library path."""

from setuptools import setup

setup(
    name='pyiqfeed',
    version='0.9',
    description='Handles connections to IQFeed, the market data feed by DTN',
    url='https://github.com/akapur/pyiqfeed',
    author='Ashwin Kapur',
    author_email='akapur@amvirk.com',
    license='GPL v2',
    packages=['pyiqfeed'],
    zip_safe=False, install_requires=['numpy'])
