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


## Releasing

jstark follows [semver](https://semver.org/). The package version is derived automatically from git tags using [hatch-vcs](https://github.com/ofek/hatch-vcs) — there is no hardcoded version in the source code.

To publish a new release:

1. Ensure all changes are merged to `main` and CI is green.
1. Create and push a tag:

   ```shell
   git tag v0.1.0
   git push origin v0.1.0
   ```

1. The `publish.yml` workflow will run tests, build the package, and publish to PyPI using trusted publishing.

Tag names **must** start with `v` followed by a semver version (e.g. `v0.1.0`, `v1.0.0`, `v2.3.1`).

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
