# Contribution Guide

jstark has been built using [Hatch](https://hatch.pypa.io/) for environment management and for its build tool, hatchling.
It is suggested that you do the same (though I suppose you don't have to) and this guide assumes that you are.

Install Hatch using the instructions at [https://hatch.pypa.io/latest/install/](https://hatch.pypa.io/latest/install/).

## Running tests

```shell
hatch run pytest
```

### Developing in Visual Studio Code

If you wish to develop in Visual Studio Code you should configure your environment to use the interpreter provided by Hatch
which can be discovered by running:

```shell
hatch run python -c "import sys;print(sys.executable)"
```

Your workspace settings (**./.vscode/settings.json**) should look something like this:

```json
{
    "python.defaultInterpreterPath": "/path/to/hatch/env/virtual/jstark/C9yWDEZH/jstark/bin/python"
}
```

### Developing in PyCharm

*Contributions welcomed here*


## Troubleshooting

### libffi-dev on wsl

Hatch is used to maintain the development environment. I started out using wsl and ran into a lot
of problems because I installed a version of python, using pyenv, without having **libffi-dev** installed.

The symptom of this was that when trying to install pyspark I got error

> Modulenotfounderror: No Module Named _ctypes for Linux System

Solved it by installing **libffi-dev** as per [https://www.pythonpool.com/modulenotfounderror-no-module-named-_ctypes-solved/](https://www.pythonpool.com/modulenotfounderror-no-module-named-_ctypes-solved/)
and *then* installing my chosen python version (which happened to be python 3.10).

### Java runtime

pyspark requires a Java runtime in order to work. Hence if you get error:

> The operation couldn’t be completed. Unable to locate a Java Runtime.

when trying to run pyspark you will need to install a Java runtime. On my mac I did
this by issuing:

```shell
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```
