gluestick [![Build Status](https://travis-ci.org/hotgluexyz/gluestick.svg?branch=master)](https://travis-ci.org/hotgluexyz/gluestick)
=============

A small Python module containing quick utility functions for standard ETL processes.

## Installation ##

```
pip install gluestick
```

## Links ##

* [Source]
* [Wiki]
* [Issues]
* [Slack]

## License ##
[MIT]

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
[Wiki]: https://github.com/hotgluexyz/gluestick/wiki
[Issues]: https://github.com/hotgluexyz/gluestick/issues
[MIT]: https://tldrlegal.com/license/mit-license
[hotglue]: https://hotglue.xyz
[Slack]: https://bit.ly/2KBGGq1
