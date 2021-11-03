from setuptools import setup

setup(
    name="binance-utils",
    version="0.0.1",
    packages=['config', 'dashboard', 'model', 'wallet'],
    install_requires=[
        'python-binance~=1.0.0',
        'sqlalchemy',
        'loguru'
    ]
)
