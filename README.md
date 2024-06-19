[![Kola Python Linux](https://github.com/jshinonome/kola/actions/workflows/release-python-linux.yml/badge.svg)](https://github.com/jshinonome/kola/actions/workflows/release-python-linux.yml)
[![Kola Python Linux Legacy](https://github.com/jshinonome/kola/actions/workflows/release-python-linux-legacy.yml/badge.svg)](https://github.com/jshinonome/kola/actions/workflows/release-python-linux-legacy.yml)

## kola

a [Polars](https://pola-rs.github.io/polars/) Interface to kdb+/q

- [Python](py-kola/README.md)
- Rust
- R

## Python Compatibility

As github have limited images for Linux, this requires at least 2.31 glibc (`rpm -qa glibc`). For lower version of glibc, use the following steps to compile python package.

1. install rustup and rust (https://www.rust-lang.org/tools/install)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup show
```

2. `git clone https://github.com/jshinonome/kola.git`

3. `cd kola/py-kola`

4. `sudo dnf install openssl-devel`

5. `make build`

6. pkg path: `../target/wheels/`
