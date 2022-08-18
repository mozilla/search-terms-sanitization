from setuptools import setup, find_packages

setup(
    name="suggest_search_tools",
    version="0.1.0",
    packages=["suggest_search_tools"],
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
