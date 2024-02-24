from setuptools import find_packages, setup

setup(
    name="cq_dagster_embedded",
    packages=find_packages(exclude=["cq_dagster_embedded_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
