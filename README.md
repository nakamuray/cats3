# cats3

`cats3` concatenate and print S3 objects matching a key (or prefix).

```
Usage: cats3 [options] key [key...]
options:
  -bucket string
        bucket name (*required*)
  -delimiter string
        delemiter to get list (default "/")
  -dry-run
        don't get object but print keys only
  -prefix
        treat args as a prefix (get all objects matching it)
  -quiet
        surpress info message
  -version
```

License: BSD-2
