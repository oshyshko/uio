# Changelog

## [1.2] - unreleased
### Fixed
- proper escaping of ` `, `+` and `%` in `file://`, `hdfs://`, `s3://` and `sftp://`
- don't lookup credentials from env (compatibility with v1.0) when there is a matching url in configuration
  given in (with-config ...) or in ~/.uio/config.clj (command line only)

### Added
- `uio` command prints statistics on SIGINFO (press Ctr+T in Terminal, OS X only)
- `uio/attrs` that works on files and directories
```
(uio/attrs "file:///")
=> {:url      "file:///"
    :dir      true
    :modified #inst"2018-01-30T23:27:56.000-00:00"
    :owner    "root"
    :group    "wheel"
    :perms    "rwxr-xr-x"}
```

- `uio/ls` accepts `{:attrs true}` that makes it return extra keys (normally returns `:url` and `:size`/`:dir`)
```
(ls "file:///" {:attrs true})
=> ({:url      "file:///Applications"
     :dir      true
     :modified #inst"2018-03-05T21:48:27.000-00:00"
     :owner    "root"
     :group    "admin"
     :perms    "rwxrwxr-x"}
    ...)
```
- `sftp://` made :known-hosts optional (if not present, disables `StrictHostKeyChecking`)
- `sftp://` added optional :skip-owner-group-lookup that keeps UID/GID as numbers
  -- this prevents `ls` from hanging for hosts that have no shell access disabled
- `sftp://` also accepts `file:///path/to/private-key` references in :identity (in addition to pasted contents of the file)
- added progress percentage reported by Ctrl+T (OS X only)
- added -c/--config <URL> option to CLI that overrides config location

### Modified
- `uio/ls` returns a vector with one entry when pointed to a file (was empty vector before) <- plays well with `uio/concat-with`
- `mem:///` now simulates directories and throws exceptions for dir-related errors (behaves like `file:///`)

### Other changes (not documented)
- `uio/concat-with` that opens multiple InputStream and returns a combined InputStream
- `uio/->countable` + `count` became => `uio/->statsable` + `uio/byte-count` due to need
  to move from `int` in `clojure.lang.Counted` to `long`.

## [1.1] - 2018-01-29
### Added
- [Command-line tool](https://github.com/oshyshko/uio/#command-line-tool)
- Support of separate credentials for multiple hosts/buckets based on URL prefix.
  Search for "Defining credentials for multiple fs and paths" in [Clojure API](https://github.com/oshyshko/uio/#clojure-api).
  This change is backward compatible with `1.0`, however the `1.0` way of specifying
  credentials is no longer documented and not recommended for use.
### Dependencies
- bumped [com.amazonaws/aws-java-sdk-s3 "1.11.66"] -> "1.11.261"
- bumped [org.apache.hadoop/hadoop-common "2.8.0"] -> "2.8.1"
- bumped [org.tukaani/xz "1.6"] -> "1.8"
- added [com.amazonaws/aws-java-sdk-sts "1.11.261"]
- added [org.apache.httpcomponents/httpclient "4.5.4"]

## [1.0] - 2017-25-07
