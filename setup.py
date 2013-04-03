from distutils.core import setup

setup(
    name="kafka-python",
    version="0.8.0-1",
    author="David Arthur",
    author_email="mumrah@gmail.com",
    url="https://github.com/mumrah/kafka-python",
    packages=["kafka"],
    license="Copyright 2012, David Arthur under Apache License, v2.0",
    description="Pure Python client for Apache Kafka",
    long_description=open("README.md").read(),
)
