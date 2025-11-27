from setuptools import setup


with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name="mongoengine-cursor-pagination",
    py_modules=["cursor_pagination"],
    version="0.2.0",
    description="Cursor based pagination for Mongoengine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Jakob Riebe",
    author_email="jr@estino.de",
    url="https://github.com/j-riebe/mongoengine-cursor-pagination",
    license="BSD",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
