from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="gluestick",
    version="2.1.23",
    description="ETL utility functions built on Pandas",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hotgluexyz/gluestick",
    install_requires=[
        "singer-python>=4.0.0",
        "numpy>=1.4",
        "pandas>=1.2.5",
        "pyarrow>=8.0.0",
        "pytz>=2022.6"
    ],
    author="hotglue",
    author_email="hello@hotglue.xyz",
    license="MIT",
    packages=["gluestick"],
    zip_safe=False,
)
