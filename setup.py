from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='gluestick',
    version='1.0.0',
    description='ETL utility functions built on Pandas',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/hotgluexyz/gluestick',
    install_requires=[
        'xlrd>=1.0.0',
        'numpy>=1.14.3',
        'pandas>=0.23.0',
    ],
    author='hotglue',
    author_email='hello@hotglue.xyz',
    license='MIT',
    packages=['gluestick'],
    zip_safe=False
)
