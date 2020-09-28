from setuptools import setup, find_packages
import pathlib

setup(
    name='feast-spark',

    version='0.0.1',

    packages=find_packages(exclude=("tests",)),  # Required

    python_requires='>=3.5, <4',

    install_requires=['pyyaml'],  # Optional

)