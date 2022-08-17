from setuptools import setup, find_packages

setup(
    name="src",
    version="0.1.0",
    install_requires=[
        "spacy",
        "presidio-analyzer",
        "pandas",
        "db-dtypes",
        "numpy",
        "numba",
        "scipy",
        "matplotlib",
        "statsmodels",
    ]
)
