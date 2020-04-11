from setuptools import setup, find_packages
import io
import os
import re

PKG_NAME = 'bow'
README_PATH = 'README.md'


def _read(*parts, **kwargs):
    filepath = os.path.join(os.path.dirname(__file__), *parts)
    encoding = kwargs.pop('encoding', 'utf-8')
    with io.open(filepath, encoding=encoding) as fh:
        text = fh.read()
    return text


def get_version():
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        _read('{}/_version.py'.format(PKG_NAME)),
        re.MULTILINE).group(1)
    return version


def get_long_description():
    return _read(README_PATH)


def get_requirements(path):
    content = _read(path)
    return [
        req
        for req in content.split("\n")
        if req != '' and not req.startswith('#')
    ]


setup(
    name=PKG_NAME,
    author='Nezar Abdennur',
    author_email='nabdennur@gmail.com',
    version=get_version(),
    license='MIT',
    description='Command line tools for Parquet and Arrow',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url='https://github.com/nvictus/bow',
    keywords=['pyarrow', 'pandas', 'arrow', 'parquet', 'cli'],
    packages=find_packages(),
    zip_safe=False,
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    install_requires=get_requirements('requirements.txt'),
    # tests_require=tests_require,
    # extras_require=extras_require,
    entry_points={
        "console_scripts": [
            "bow  = bow.__main__.cli"
        ]
    }
)
