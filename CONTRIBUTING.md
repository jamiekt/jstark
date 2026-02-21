# Contribution Guide

jstark uses [uv](https://docs.astral.sh/uv/) for environment management and [hatchling](https://hatch.pypa.io/) as the build backend.

Install uv using the instructions at [https://docs.astral.sh/uv/getting-started/installation/](https://docs.astral.sh/uv/getting-started/installation/).

## Running tests

```shell
uv sync
uv run pytest
```

### Developing in Visual Studio Code

uv creates a standard `.venv/` directory in the project root. VS Code should detect this automatically. If it doesn't, set the interpreter path in your workspace settings (**./.vscode/settings.json**):

```json
{
    "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python"
}
```

### Developing in PyCharm

*Contributions welcomed here*


## Troubleshooting

### libffi-dev on wsl

I started out using wsl and ran into a lot
of problems because I installed a version of python, using pyenv, without having **libffi-dev** installed.

The symptom of this was that when trying to install pyspark I got error

> Modulenotfounderror: No Module Named _ctypes for Linux System

Solved it by installing **libffi-dev** as per [https://www.pythonpool.com/modulenotfounderror-no-module-named-_ctypes-solved/](https://www.pythonpool.com/modulenotfounderror-no-module-named-_ctypes-solved/)
and *then* installing my chosen python version (which happened to be python 3.10).

### Java runtime

pyspark requires a Java runtime in order to work. Hence if you get error:

> The operation couldn't be completed. Unable to locate a Java Runtime.

when trying to run pyspark you will need to install a Java runtime. On my mac I did
this by issuing:

```shell
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
```
