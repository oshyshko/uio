# Changelog

## [1.2] - unreleased
### Added
- `uio/attrs` that works on files and directories
- `uio/ls` accepts `{:attrs true}` that makes it return extra keys (normally returns `:url` and `:size`/`:dir`)
- `uio` command prints statistics on SIGINFO (press Ctr+T in Unix terminal)

### Modified
- `uio/ls` returns a vector with one entry when pointed to a file (was empty vector before)
- `mem:///` now simulates directories and throws exceptions for dir-related errors (behaves like `file:///`)

### Fixed
- escaping of ` `, `+` and `%` in URLs
- `file:///` now works with ` `, `+` and `%`

### Changes in experimental API (not documented)
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
