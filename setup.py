from setuptools import setup

setup(
    install_requires=[line.rstrip() for line in open('requirements.txt')],
)
