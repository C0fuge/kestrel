#!/usr/bin/env python3
"""Verify and preprocess the actual generated decoder compile command before native compilation."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess


ISA = {"-march=x86-64", "-mtune=generic"}


def write(path, value):
    """Keep new probe evidence without overwriting another phase."""
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")


def command(entry, compiler):
    """Reject overrides and remove only compile-output/dependency actions from actual argv."""
    argv = entry.get("arguments") or shlex.split(entry["command"])
    if not argv or Path(argv[0]).name != ("g++" if compiler == "gcc" else "clang++"):
        raise ValueError("Unexpected actual decoder compiler")
    result = [argv[0]]
    index = 1
    while index < len(argv):
        value = argv[index]
        if value in ("-D", "-U"):
            index += 1
            if index == len(argv):
                raise ValueError("Incomplete macro argument")
            value += argv[index]
        if value.startswith(("@", "-Wp,")) or value in ("-Xclang", "-Xpreprocessor", "--", "-include", "-imacros"):
            raise ValueError("Unreviewed compiler passthrough")
        if value.startswith("-m") and value not in ISA:
            raise ValueError("Unreviewed actual ISA flag: " + value)
        if value.startswith("-O") and value != "-O3":
            raise ValueError("Unreviewed actual optimization flag")
        if value.startswith("-std=") and value != "-std=gnu++11":
            raise ValueError("Actual C++ standard override")
        if "SNAPPY_NEW_VERSION" in value or ("_GLIBCXX_USE_CXX11_ABI" in value and value != "-D_GLIBCXX_USE_CXX11_ABI=1"):
            raise ValueError("Actual decoder/ABI override")
        if value.startswith("-stdlib=") and value != "-stdlib=libstdc++":
            raise ValueError("Actual standard-library override")
        if value == "-UNDEBUG" or value.startswith(("-D_GLIBCXX_DEBUG", "-U_GLIBCXX_DEBUG")):
            raise ValueError("Actual release/library ABI override")
        if value.startswith(("-fabi-", "-fclang-abi-", "-fshort-", "-fpack-struct", "--optimize")):
            raise ValueError("Unreviewed ABI/optimization override")
        if value in ("-o", "-MF", "-MT", "-MQ"):
            index += 2
            continue
        if value in ("-c", "-MD", "-MMD", "-MP"):
            index += 1
            continue
        if value.startswith(("-o", "-MF", "-MT", "-MQ")) and len(value) > 2:
            raise ValueError("Unreviewed attached output/dependency option")
        result.append(value)
        index += 1
    required = ISA | {"-D_GLIBCXX_USE_CXX11_ABI=1", "-std=gnu++11", "-O3", "-DNDEBUG"}
    if not required.issubset(result) or result.count(entry["file"]) != 1:
        raise ValueError("Actual decoder compile contract is incomplete")
    return result


def common_arguments(argv, compiler, toolchain, installation):
    """Require exact recorded driver selectors and remove only those compiler differences."""
    root, install = Path(toolchain), Path(installation)
    if not root.is_absolute() or not install.is_absolute() or ".." in root.parts + install.parts or not install.is_relative_to(root):
        raise ValueError("Invalid recorded GCC installation")
    expected = ["--no-default-config", "--gcc-install-dir=" + installation, "--gcc-toolchain=" + toolchain]
    selectors = [value for value in argv[1:] if value.startswith(
        ("--gcc-", "-gcc-", "--no-default-config", "--config"))]
    if (compiler == "clang" and (sorted(selectors) != sorted(expected))) or (compiler == "gcc" and selectors):
        raise ValueError("Unexpected or missing compiler configuration/toolchain selector")
    return [value for value in argv[1:] if value not in expected]


def retain_default_config(argv, compiler, directory, output):
    """Retain any automatic Clang configuration reported without the disabling selector."""
    version, diagnostic = capture([argv[0], "--version"], directory, output / "default-compiler-version.txt")
    configs = []
    for index, name in enumerate(re.findall(r"^Configuration file: (.+)$", version + diagnostic, re.M)):
        path = Path(name).resolve(strict=True)
        content = path.read_bytes()
        retained = "default-config-" + str(index) + ".cfg"
        with (output / retained).open("xb") as stream:
            stream.write(content)
        configs.append({"path": str(path), "retained": retained, "sha256": hashlib.sha256(content).hexdigest()})
    write(output / "default-config.json", {"compiler": compiler, "automatic_configs": configs,
          "disabled_in_actual_argv": "--no-default-config" in argv})


def capture(argv, cwd, path):
    """Retain exact argv, status and full preprocessing output before requiring success."""
    try:
        result = subprocess.run(argv, cwd=cwd, capture_output=True, text=True, timeout=120)
    except subprocess.TimeoutExpired as error:
        def text(value):
            return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
        result = subprocess.CompletedProcess(argv, 124, text(error.stdout), text(error.stderr))
    with path.open("x") as stream:
        stream.write(result.stdout)
    with path.with_suffix(path.suffix + ".stderr").open("x") as stream:
        stream.write(result.stderr)
    write(path.with_suffix(path.suffix + ".command.json"), {"argv": argv, "cwd": cwd, "status": result.returncode})
    if result.returncode:
        raise ValueError("Actual decoder probe failed")
    return result.stdout, result.stderr


def probe(database, compiler, output):
    """Derive compiler, flags, macros, include paths and standard library from actual snappy.cc argv."""
    entries = [item for item in json.loads(database.read_text())
               if item["file"].endswith("/third_party/snappy/snappy.cc")]
    if len(entries) != 1:
        raise ValueError("Expected one actual decoder compilation")
    entry = entries[0]
    argv = command(entry, compiler)
    common_arguments(argv, compiler, (output / "gcc-toolchain-root.txt").read_text().strip(),
                     (output / "gcc-install-dir.txt").read_text().strip())
    retain_default_config(argv, compiler, entry["directory"], output)
    macros, _ = capture(argv + ["-E", "-dM"], entry["directory"], output / "macros.txt")
    values = dict(re.findall(r"^#define (\w+) (.*)$", macros, re.M))
    expected = "false" if compiler == "gcc" else "true"
    if values.get("SNAPPY_NEW_VERSION") != expected or ("__clang__" in values) != (compiler == "clang"):
        raise ValueError("Actual compiler/decoder macros differ")
    if values.get("__cplusplus") != "201103L" or values.get("_GLIBCXX_USE_CXX11_ABI") != "1":
        raise ValueError("Actual standard/ABI macros differ")
    if not values.get("__GLIBCXX__") or "__FAST_MATH__" in values:
        raise ValueError("Unexpected actual standard library or fast-math mode")
    if "NDEBUG" not in values or any(values.get(name) not in (None, "0", "false") for name in
            ("SNAPPY_HAVE_SSSE3", "SNAPPY_HAVE_BMI2", "SNAPPY_HAVE_X86_CRC32", "SNAPPY_HAVE_NEON", "SNAPPY_HAVE_NEON_CRC32")):
        raise ValueError("Actual decoder requires unreviewed debug/ISA settings")
    if not values.get("__x86_64__") or any(name in values for name in
            ("__AVX__", "__AVX2__", "__BMI__", "__BMI2__", "__SSSE3__", "__SSE3__", "__SSE4_1__", "__SSE4_2__")):
        raise ValueError("Actual ISA exceeds generic x86-64")
    _, trace = capture(argv + ["-E", "-H", "-o", "/dev/null"], entry["directory"], output / "include-trace.txt")
    headers = {Path(value).resolve(strict=True) for value in re.findall(r"^\.+ (.+/bits/c\+\+config\.h)$", trace, re.M)}
    if len(headers) != 1:
        raise ValueError("Actual libstdc++ configuration header is unproven")
    header = headers.pop()
    header_hash = hashlib.sha256(header.read_bytes()).hexdigest()
    driver = [value for value in argv if value != entry["file"]]
    library, _ = capture(driver + ["-print-file-name=libstdc++.so"], entry["directory"], output / "libstdcxx.txt")
    library = Path(library.strip()).resolve(strict=True)
    with library.open("rb") as stream:
        library_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    capture(driver + ["--version"], entry["directory"], output / "actual-compiler-version.txt")
    write(output / "effective.json", {"compiler": compiler, "entry": entry, "probe_argv": argv,
          "compile_database_sha256": hashlib.sha256(database.read_bytes()).hexdigest(),
          "decoder": expected, "abi": values["_GLIBCXX_USE_CXX11_ABI"], "cplusplus": values["__cplusplus"],
          "glibcxx_header_version": values.get("__GLIBCXX__"), "glibcxx_config_header": str(header),
          "glibcxx_config_sha256": header_hash, "libstdcxx": str(library), "libstdcxx_sha256": library_hash,
          "abi_features": {name: values.get(name) for name in
              ("__EXCEPTIONS", "__GXX_RTTI", "_GLIBCXX_DEBUG", "_GLIBCXX_ASSERTIONS", "_FORTIFY_SOURCE", "NDEBUG")}})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("compiler", choices=("gcc", "clang"))
    parser.add_argument("database", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    probe(args.database, args.compiler, args.output)
