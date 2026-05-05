# kola

[![Kola CI](https://github.com/jshinonome/kola/actions/workflows/CI.yml/badge.svg)](https://github.com/jshinonome/kola/actions/workflows/CI.yml)
[![PyPI](https://img.shields.io/pypi/v/kola)](https://pypi.org/project/kola/)
[![License](https://img.shields.io/github/license/jshinonome/kola)](LICENSE)

A [Polars](https://docs.pola.rs/) interface to kdb+/q, powered by Rust.

## Overview

**kola** provides high-performance connectivity between Python/Polars and kdb+/q processes. The core is written in Rust for speed and safety, with Python bindings via [PyO3](https://pyo3.rs/).

### Features

- Synchronous and asynchronous queries to kdb+/q
- Full kdb+ IPC protocol v6
- Bi-directional type mapping between Polars DataFrames and q tables
- TLS encryption support
- Automatic retry with exponential backoff
- Subscription support for real-time data
- Read kdb+ binary (splayed) tables directly into Polars DataFrames
- Serialize data as kdb+ IPC bytes without a connection

## Project Structure

| Directory       | Description                              |
| --------------- | ---------------------------------------- |
| `crates/kola`   | Core Rust library (connector, IPC serde) |
| `py-kola`       | Python bindings (PyO3 + pyo3-polars)     |

## Installation

```bash
pip install kola
```

**Requirements**: Python ≥ 3.10, Polars ≥ 1.31.0, PyArrow ≥ 20.0.0

## Quick Start

```python
import polars as pl
import kola

conn = kola.Q('localhost', 1800)

# query
df = conn.sync("select from trade where date=last date")

# send data
conn.sync("upsert", "table", df)
```

See [py-kola/README.md](py-kola/README.md) for full API documentation and type mapping reference.

## License

[BSD-3-Clause](LICENSE)
