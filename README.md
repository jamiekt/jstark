# jstark

[![PyPI - Version](https://img.shields.io/pypi/v/jstark.svg)](https://pypi.org/project/jstark)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/jstark.svg)](https://pypi.org/project/jstark)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install jstark
```

## License

`jstark` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

## Troubleshooting

Hatch is used to maintain the development environment. I started out using wsl and ran into a lot
of problems because I installed a version of python, using pyenv, without having **libffi-dev** installed.

The symptom of this was that when trying to install pyspark I got error

> Modulenotfounderror: No Module Named _ctypes for Linux System

Solved it by installing **libffi-dev** as per https://www.pythonpool.com/modulenotfounderror-no-module-named-_ctypes-solved/
and *then* installing my chosen python version (which happened to be python 3.10). 