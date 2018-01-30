# Changelog

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
