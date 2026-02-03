# Schema Evolution Test: DataFusion with Parquet vs Vortex

This project demonstrates schema evolution issues in Apache DataFusion when reading multiple files with incompatible schemas. It compares the behavior between standard Parquet format and Vortex format.

## Result
### Parquet
```shell
cargo r --example parquet
```
```
+----+------+-------+
| id | code | value |
+----+------+-------+
| 1  | A100 | 100   |
| 2  | B200 | 200   |
| 3  | C300 | 300   |
| 4  | 400  | 400   |
| 5  | 500  | 500   |
| 6  | 600  | 600   |
+----+------+-------+
Query succeeded unexpectedly
```


### Vortex
```shell
cargo r --example vortex
```
```
thread 'tokio-runtime-worker' (110661964) panicked at /Users/xxx/.cargo/git/checkouts/vortex-0eff073da78b9952/d9fffbe/vortex-error/src/lib.rs:310:33:
Failed to convert scalar to utf8:
  Expected a string scalar, found Primitive(I64(400))
Backtrace:
```