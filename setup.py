from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="gluestick",
    version="3.0.0",
    description="ETL utility functions built for the hotglue iPaaS platform",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hotgluexyz/gluestick",
    install_requires=[
        "singer-python>=4.0.0",
        "numpy>=1.4",
        "pandas>=1.2.5",
        "pyarrow>=8.0.0",
        "pytz>=2022.6",
        "polars==1.34.0"
    ],
    author="hotglue",
    author_email="hello@hotglue.xyz",
    license="MIT",
    packages=["gluestick", "gluestick.*"],
    zip_safe=False,
)
