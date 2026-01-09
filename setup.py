from setuptools import find_packages, setup

setup(
    name="dagster_Test",
    packages=find_packages(exclude=["dagster_Test_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
