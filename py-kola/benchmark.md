### Env

- python: 3.11.5
- kola: 0.2.1 + polars: 0.19.14
- pykx: 2.2.0

### Comparison

| Case | column num |     operation     | kola + polars       | mem(MB) | pykx             | mem(MB) |  speed |
| ---- | ---------- | :---------------: | ------------------- | ------- | ---------------- | ------- | -----: |
| 1    | 14         |  query from kdb   | 301 ms ± 4.25 ms    | 348     | 381 ms ± 8.52 ms | 632     |  1.27x |
| 1    | 14         |    send to kdb    | 387 ms ± 8.75 ms    | 708     | 267 ms ± 11.5 ms | 632     |  0.69x |
| 1    | 14         |   cast to pd df   | 57.1 ms ± 1.85 ms   | 976     | 1.36 s ± 39.8 ms | 894     | 23.82x |
| 1    | 14         | send pd df to kdb | 506 ms ± 20.6 ms ms | 1203    | 2.73 s ± 95.9 ms | 1093    |  5.40x |
| 2    | 64         |  query from kdb   | 973 ms ± 18.1 ms    | 1183    | 1.39 s ± 22.9 ms | 2170    |  1.43x |
| 2    | 64         |    send to kdb    | 1.21 s ± 42.9 ms    | 1337    | 726 ms ± 46.2 ms | 2170    |  0.60x |
| 2    | 64         |   cast to pd df   | 201 ms ± 6.23 ms    | 1523    | 1.31 s ± 9.31 ms | 2203    |  6.52x |
| 2    | 64         | send pd df to kdb | 1.48 s ± 66.5 ms    | 1896    | 3.1 s ± 102 ms   | 3379    |  2.09x |
| 3    | 5 (3+5+5)  |  query from kdb   | 397 ms ± 11.1 ms    | 484     | 466 ms ± 34.4 ms | 694     |  1.17x |
| 3    | 5 (3+5+5)  |   cast to pd df   | 748 ms ± 23.9 ms    | 863     | 1.56 s ± 70.7 ms | 1092    |  2.09x |

- mem(MB): python process total memory usage
- pd: pandas

> note:
>
> - kola sends data to kdb+ is slower than pykx
> - kola sends pandas data to kdb+ is faster, as polars converts pandas data much faster

#### Case 1

```
n: 2000000;
q: ([]sym: n?`3; time: .z.D + 1000 * "n"$til n; volume: n?1000; cond: n # enlist "aaa");
columns: `$("ask"; "bid") cross string til 5;
q: ![q; (); 0b; columns!(count columns)#enlist (?;n;1.0)];
```

#### Case 2

```
n: 2000000;
q: ([]sym: n?`3; time: .z.D + 1000 * "n"$til n; volume: n?1000; cond: n # enlist "aaa");
columns: `$("ask"; "bid") cross string til 30;
q: ![q; (); 0b; columns!(count columns)#enlist (?;n;1.0)];
```

#### Case 3

```
n: 2000000;
dq: ([] sym: n?`3; time: .z.D + 1000 * "n"$til n; volume: n?1000; ask: n # enlist 5?1.0; bid: n # enlist 5?1.0 );
```
