from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='gluegun',
    version='1.0.5',
    description='ETL utility functions built on Pandas',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/hotgluexyz/gluegun',
    install_requires=[
        'xlrd==1.2.0',
        'numpy>=1.14.3',
        'pandas>=0.23.0',
    ],
    author='hotglue',
    author_email='hello@hotglue.xyz',
    license='MIT',
    packages=['gluegun'],
    zip_safe=False
)
