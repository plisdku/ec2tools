from setuptools import setup

install_requires = ["boto3", "jsonpath_rw", "sshconf"]

version = "0.0.1"

setup(name="ec2tools",
    version=version,
    description="Paul's EC2 utilities",
    author="Paul Hansen",
    author_email="paul.c.hansen@gmail.com",
    packages=["ec2tools"],
    install_requires= install_requires,
    python_requires=">=3")