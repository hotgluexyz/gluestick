# Gluestick #
![PyPI Version](https://img.shields.io/pypi/v/gluestick)
[![License](https://img.shields.io/pypi/l/gluestick)](https://github.com/hotgluexyz/gluestick/blob/master/LICENSE)

A Python library for efficient ETL processes, optimized for hotglue

## Installation ##

```
pip install gluestick
```

## Links ##

* [Source]
* [Issues]
* [Slack]

## Dependencies ##
* NumPy
* Pandas

## Memory benchmarking (local development) ##

The repo includes a script that prints **peak RSS** (resident set size) in MiB for several gluestick workloads. It uses the same scenarios as `tests/function_tests/test_memory_usage.py`, so you can compare numbers before and after a change on **your machine**. Peak RSS is a rough signal, not a portable “this library always uses X MB” guarantee.

**Setup (from the repository root):**

```bash
pip install ".[test]"
```

This installs gluestick, pytest, and `memory-profiler`, which the script and memory tests need.

**Run the benchmark:**

```bash
python scripts/memory_benchmark.py
```

For machine-readable output (e.g. to save and diff):

```bash
python scripts/memory_benchmark.py --json > before.json
# change code, then:
python scripts/memory_benchmark.py --json > after.json
```

Compare the JSON objects (or use `diff` / `jq`) to see per-scenario peak MiB before and after. Percent change is only meaningful when both runs use the same host and similar load.

**Sanity-check with tests:**

```bash
pytest tests/function_tests/test_memory_usage.py -q
```

If pytest passes, the same workloads stay within the smoke-test RSS bands used in CI.

## Contributing ##
This project is maintained by the [hotglue] team. We welcome contributions from the 
community via issues and pull requests.

If you wish to chat with our team, feel free to join our [Slack]!


[Source]: https://github.com/hotgluexyz/gluestick
[Issues]: https://github.com/hotgluexyz/gluestick/issues
[MIT]: https://tldrlegal.com/license/mit-license
[hotglue]: https://hotglue.com
[Slack]: https://bit.ly/2KBGGq1
