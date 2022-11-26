# Contribution Guide

jstark has been built using [Hatch](https://hatch.pypa.io/) for environment management and for its build system, hatchling.
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
