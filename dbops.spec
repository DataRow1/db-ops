# dbops.spec
# Robust PyInstaller spec for dbops (BRICK-OPS)
#
# Why this exists:
# - Typer/Click/Questionary use dynamic imports and package data.
# - PyInstaller often misses these unless we explicitly collect submodules + data files.

from PyInstaller.utils.hooks import (
    collect_all,
    collect_data_files,
    collect_submodules,
)
import os
import sys
import sysconfig

def _default_pathex() -> list[str]:
    """Compute a cross-platform pathex.

    Why:
    - Homebrew/Linux builds often run in isolated environments where `site.getsitepackages()`
      may be empty/unavailable or point at locations you don't want.
    - PyInstaller only needs to find *your* sources and the environment's site-packages.
    """

    paths: list[str] = []

    # Project roots
    paths.append(os.path.abspath("."))
    paths.append(os.path.abspath("src"))

    # Current interpreter's site-packages (works on Linux/macOS/Windows)
    try:
        purelib = sysconfig.get_paths().get("purelib")
        platlib = sysconfig.get_paths().get("platlib")
        for p in (purelib, platlib):
            if p:
                paths.append(os.path.abspath(p))
    except Exception:
        pass

    # If a venv is active, also include its site-packages explicitly
    venv = os.environ.get("VIRTUAL_ENV")
    if venv:
        candidates = [
            os.path.join(venv, "lib", f"python{sys.version_info.major}.{sys.version_info.minor}", "site-packages"),
            os.path.join(venv, "Lib", "site-packages"),
        ]
        for c in candidates:
            if os.path.isdir(c):
                paths.append(os.path.abspath(c))

    # De-dup while preserving order
    return list(dict.fromkeys(paths))


PATHEX = _default_pathex()


def _collect_all(pkg: str):
    """Collect datas, binaries, and hiddenimports for a package.

    NOTE: In the PyInstaller version you're using, collect_all returns a tuple:
      (datas, binaries, hiddenimports)
    """

    datas, binaries, hidden = collect_all(pkg)
    return list(datas), list(binaries), list(hidden)


def _collect_pkg(pkg: str):
    """Collect a package in the most robust way.

    We combine:
    - collect_all(pkg): binaries/datas/hiddenimports
    - collect_submodules(pkg): to force-import all submodules (dynamic imports)
    - collect_data_files(pkg): to include non-python assets used at runtime

    Returns (datas, binaries, hiddenimports).
    """

    datas, binaries, hidden = _collect_all(pkg)

    # Force all submodules to be bundled (prevents 'No module named ...')
    hidden += collect_submodules(pkg)

    # Include package data files (prompt_toolkit + questionary rely on these)
    datas += collect_data_files(pkg, include_py_files=False)

    # De-dup for stability
    hidden = sorted(set(hidden))
    datas = list(dict.fromkeys(datas))
    binaries = list(dict.fromkeys(binaries))

    return datas, binaries, hidden


# Core CLI deps
TY_DATAS, TY_BINARIES, TY_HIDDEN = _collect_pkg("typer")
CL_DATAS, CL_BINARIES, CL_HIDDEN = _collect_pkg("click")
RI_DATAS, RI_BINARIES, RI_HIDDEN = _collect_pkg("rich")

# TUI deps
QA_DATAS, QA_BINARIES, QA_HIDDEN = _collect_pkg("questionary")
PT_DATAS, PT_BINARIES, PT_HIDDEN = _collect_pkg("prompt_toolkit")

# Databricks SDK (namespace package structure can be tricky)
DB_DATAS, DB_BINARIES, DB_HIDDEN = _collect_pkg("databricks")


datas = []
binaries = []
hiddenimports = []

for d, b, h in [
    (TY_DATAS, TY_BINARIES, TY_HIDDEN),
    (CL_DATAS, CL_BINARIES, CL_HIDDEN),
    (RI_DATAS, RI_BINARIES, RI_HIDDEN),
    (QA_DATAS, QA_BINARIES, QA_HIDDEN),
    (PT_DATAS, PT_BINARIES, PT_HIDDEN),
    (DB_DATAS, DB_BINARIES, DB_HIDDEN),
]:
    datas += d
    binaries += b
    hiddenimports += h

# Extra explicit imports that are commonly missed with Typer
hiddenimports += [
    "typer.main",
    "typer.core",
    "typer.params",
    "typer.rich_utils",
    "click.termui",
    "click.decorators",
]

hiddenimports = sorted(set(hiddenimports))


a = Analysis(
    ["src/dbops_cli/__main__.py"],
    pathex=PATHEX,
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[
        # Optional SSH bits in prompt_toolkit that pull in asyncssh.
        "asyncssh",
        "prompt_toolkit.contrib.ssh",
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="dbops",
    debug=False,
    strip=False,
    upx=False,
    console=True,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="dbops",
)