from setuptools import find_packages, setup

setup(
    name="clinimatch_Hermes",
    packages=find_packages(exclude=["clinimatch_Hermes_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "PyGithub",
        "matplotlib",
        "pandas",
        "nbconvert",
        "nbformat",
        "ipykernel",
        "jupytext",
        "json",
        "plotly.express",
        "python-dotenv",

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
