# Bundled demo notebook for jstark

GitHub issue: [#144](https://github.com/jamiekt/jstark/issues/144)

## Goal

Ship a Jupyter notebook (`GroceryFeatures_demo.ipynb`) inside the `jstark`
wheel that demonstrates the basic features of `GroceryFeatures` using the
`FakeGroceryTransactions` sample data. Provide a `jstark-demo` CLI that copies
the bundled notebook to the user's current working directory, and make Jupyter
itself installable via a `jupyter` extra.

## User workflow

```shell
pip install jstark[jupyter]
jstark-demo
jupyter notebook GroceryFeatures_demo.ipynb
```

`pip install jstark[jupyter]` installs `notebook` and `faker` as transitive
dependencies. `jstark-demo` copies the bundled notebook into the current
directory. The user launches Jupyter themselves.

## Components

### 1. Notebook — `jstark/demo/GroceryFeatures_demo.ipynb`

Committed to the repository and included in the wheel. Narrative walkthrough
(~12 cells, markdown interleaved with code) covering:

1. Title + intro markdown — what jstark is, the `as_at` concept in one
   paragraph, link back to the project README.
2. Setup cell — imports (`date`, `FakeGroceryTransactions`, `GroceryFeatures`).
3. Sample data markdown — one sentence on what `FakeGroceryTransactions`
   produces.
4. Sample data code — `input_df = FakeGroceryTransactions().df`,
   `input_df.show(5)`, `input_df.printSchema()`.
5. Mnemonic markdown — explain `{start}{unit}{end}` with examples (`3m1`,
   `6m4`, `1q1`).
6. Build features code —
   `GroceryFeatures().with_as_at(date(2022, 1, 1)).with_feature_periods(["3m1", "6m4"])`.
7. Aggregate code —
   `output_df = input_df.groupBy("Store").agg(*gf.features)`
   plus `output_df.select(...).show()`.
8. Descriptions markdown — "every feature carries a description in its
   metadata".
9. Descriptions code — print name/description pairs for one period.
10. References markdown — "inspect what input columns a feature requires".
11. References code — `gf.references["BasketCount_3m1"]` and one or two others.
12. Closing markdown — pointer to the Features reference in the README for the
    full list.

No pandas conversion. No plotting.

### 2. CLI — `jstark/demo/cli.py`

Entry point: `jstark-demo`. Implementation uses only the standard library
(`argparse`, `shutil`, `importlib.resources`, `pathlib`, `sys`).

Surface:

```
jstark-demo [-f | --force] [-h | --help]
```

Behaviour:

```
locate bundled notebook via
    importlib.resources.files("jstark.demo") / "GroceryFeatures_demo.ipynb"
target = Path.cwd() / "GroceryFeatures_demo.ipynb"
if target.exists() and not args.force:
    print to stderr:
        "GroceryFeatures_demo.ipynb already exists; refusing to overwrite. "
        "Use --force to overwrite."
    exit 1
else:
    shutil.copyfile(source, target)
    print to stdout:
        "Copied to ./GroceryFeatures_demo.ipynb — run "
        "'jupyter notebook GroceryFeatures_demo.ipynb' to open it."
    exit 0
```

### 3. Packaging — `pyproject.toml`

Additions:

```toml
[project.optional-dependencies]
faker = ["faker>=40.0.0"]
jupyter = ["notebook", "faker>=40.0.0"]

[project.scripts]
jstark-demo = "jstark.demo.cli:main"
```

Notebook location: `jstark/demo/GroceryFeatures_demo.ipynb`, alongside
`jstark/demo/__init__.py` and `jstark/demo/cli.py`.

Verify during implementation that hatchling's default wheel build includes the
`.ipynb` file under the package directory. If not, add an explicit include
rule under `[tool.hatch.build.targets.wheel]`. Check with
`uv build && unzip -l dist/*.whl`.

### 4. Tests

**`tests/test_demo_notebook.py`** — executes the committed notebook
end-to-end via `nbclient.NotebookClient` and asserts no cell raises. Targets
`jstark/demo/GroceryFeatures_demo.ipynb` at its source location so it tests
what is committed.

**`tests/test_demo_cli.py`** — three cases, all invoking the installed
`jstark-demo` script via `subprocess.run(["jstark-demo", ...], cwd=tmp_path)`.
This tests the real entry point wired up by `[project.scripts]`, available
because the project is installed into the dev environment by `uv sync`.

- Happy path: target does not exist, CLI copies and exits 0, file exists at
  target path and matches source bytes.
- Refusal: target exists, CLI exits 1, original file is untouched, error
  message is on stderr.
- Force: target exists, `--force` overrides, CLI exits 0, file is overwritten.

Dev dependencies to add: `nbclient`, and `ipykernel` (the kernel nbclient
needs). Confirm minimum set during implementation.

### 5. README updates

Add a short section after Quick start:

````markdown
### Try the demo notebook

```shell
pip install jstark[jupyter]
jstark-demo
jupyter notebook GroceryFeatures_demo.ipynb
```
````

## Error handling

- **Target exists without `--force`** — handled as above.
- **Bundled notebook missing from installed package** — packaging bug, not a
  user error. Let `importlib.resources` / `shutil.copyfile` raise. No
  try/except swallowing.
- **CWD not writable** — let `shutil.copyfile` raise `PermissionError`. No
  rewrapping.

## Non-goals (YAGNI)

- No auto-launch of Jupyter from `jstark-demo`.
- No `--dest DIR` flag (always CWD).
- No interactive overwrite prompt (`--force` only).
- No `jstark.demo_notebook_path()` helper on the main package namespace; the
  notebook is reached exclusively via the CLI.
- No matplotlib / pandas / visualisation dependencies in the notebook.
- No compatibility shims — the CLI and notebook target Python 3.10+
  (matching the rest of jstark).
