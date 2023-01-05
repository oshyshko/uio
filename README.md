`[uio/uio "1.1"]` -- [see what's new](CHANGELOG.md)

# uio

`uio` is a [Clojure library](#clojure-api) and a [command line tool](#command-line-tool) for accessing HDFS, S3, SFTP and other file systems.

Features:
- uses URLs everywhere (there are no relative paths or current working directories)
- minimalistic: built around 7 core functions, has no global state or def-macros
- can automatically encode/decode files based on their extension
- allows adding new protocols and codecs from REPL or user code
- as a command line tool, respects stdin/stdout/stderr streams and exit codes

## Built-in protocols

|                  |from | to  |size |exists?|delete|mkdir| ls  |attrs| URL format                             |
|------------------|:---:|:---:|:---:|:-----:|:----:|:---:|:---:|:---:|----------------------------------------|
|[file](#file)     |  •  |  •  |  •  |   •   |  •   |  •  |  •  |  •  |`file:///path/to/file.txt`              |
|[hdfs](#hdfs)     |  •  |  •  |  •  |   •   |  •   |  •  |  •  |  •  |`hdfs://[host]/path/to/file.txt`        |
|[http(s)](#https) |  •  |     |:cat:| :cat: |      |     |     |     |`http[s]://host[:port]/path/to/file.txt`|
|[mem](#mem)       |  •  |  •  |  •  |   •   |  •   |  •  |  •  |  •  |`mem:///path/to/file.txt`               |
|[res](#res)       |  •  |     |     |   •   |      |     |     |     |`res:///com/mypackage/file.txt`         |
|[s3](#s3)         |  •  |  •  |  •  |   •   |  •   |:dog:|  •  |  •  |`s3://bucket/key/with/slashes.txt`      |
|[sftp](#sftp)     |  •  |:bug:|  •  |   •   |:pig: |  •  |  •  |  •  |`sftp://host[:port]/path/to/file.txt`   |

- :cat: - limited support, see [HTTP(S)](#https)
- :dog: - these FS don't have directories: `ls` will emulate directories, `mkdir` will do nothing
- :bug: - the outbound content will be accumulated in a temporary `.gz` file and will be uploaded via SFTP when the stream is closed (a workaround to a concurrency bug in JSch)
- :pig: - for files only (won't delete directories)

Built-in codecs: `.bz2`, `.gz`, `.xz`.


## Clojure API

```clojure
; add [uio/uio "1.1"] to your :dependencies

(ns example
  (:require [uio.uio :as uio]))

; Reading and writing
(slurp (uio/from "hdfs:///path/to/file.txt"))                     ; => "<content>"
(spit  (uio/to   "s3://bucket/key/with/slashes.txt") "<content>") ; => nil

; NOTE: `slurp` reads entire content into memory and it is only suitable for small files.

; NOTE: Streams returned by `from` and `to` should be closed by the caller (you).
;       Fns `slurp` and `split` will call `(.close ...)` automatically.
;       For other cases, it's highly recommended to use `with-open` macro
;       (it also calls `(.close ...)` on streams passed to it).
;       This approach will save your code from resource leaks or data corruption.

; Read the first line of some HTML
(with-open [is (uio/from "http://www.google.com/")]
  (->> (line-seq (clojure.java.io/reader is))
       (take 1)
       (doall)))
; => ("<!doctype ...")

; Write an image to a file
(with-open [os (uio/to "file:////path/to/green-dot.gif")]
  (.write os (byte-array [0x47 0x49 0x46 0x38 0x37 0x61 0x01 0x00
                          0x01 0x00 0x80 0x00 0x00 0x00 0xd6 0x7e
                          0x00 0x00 0x00 0x2c 0x00 0x00 0x00 0x00
                          0x01 0x00 0x01 0x00 0x00 0x02 0x02 0x44
                          0x01 0x00 0x3b])))
; => nil

; Reading and writing with extension codecs
(slurp (uio/from* "file:///path/to.gz"))                    ; decompress with `gzip`
(spit  (uio/to*   "file:///path/to.bz2") "<content>")       ; compress with `bzip2`
(spit  (uio/to*   "file:///path/to.xz.bz2.gz") "<content>") ; compress with `xz`, 'bzip2' and `gzip`

; Getting file size
(uio/size "file:///path/to/file.txt")                       ; => Number

; Testing a file for existence
(uio/exists? "file:///path/to/file.txt")                    ; => boolean

; Deleting individual files and directories
(uio/delete "file:///path/to/file.txt")                     ; => nil
(uio/delete "file:///path/to")                              ; => nil

; Listing directory contents
(->> (uio/ls "file:///path/to")                             ; returns a lazy sequence. Potentially,
     (take 4))                                              ; ... a very large one, thus, using
                                                            ; ... (take ...) is not a bad idea.
                                                            ;
; => [{:url     "file:///path/to/some-dir"                  ; a dir
;      :dir     true}
;     {:url     "file:///to/file.txt"                       ; a file (has :size)
;      :size    123}
;     {:url     "file:///path/to/file2.txt"                 ; a symlink
;      :symlink "file:///path/to/file.txt"
;      :size    123}]

; Combine with `filter` and `map` to get the output you want
(->> (uio/ls "s3://bucket/path/to/")
     (remove :dir)                                          ; remove dirs
     (map :url)                                             ; leave URLs only
     (take 3))
; => ["s3://bucket/path/to/file1.txt"
;     "s3://bucket/path/to/file2.txt"
;     "s3://bucket/path/to/file3.txt"]

; Listing files recursively
(->> (uio/ls "s3://bucket/path/to/" {:recurse true})        ; override default `false` value
     (remove :dir)                                          ; leave files only
     (map :size)                                            ; leave sizes only
     (reduce +))                                            ; get total size (in bytes)
; => 12345678

; Getting attributes for a single file
(uio/attrs "file:///")
; => {:url      "file:///"
;     :dir      true
;     :modified #inst"2018-01-30T23:27:56.000-00:00"
;     :owner    "root"
;     :group    "wheel"
;     :perms    "rwxr-xr-x"}

; Listing with extra attributes
(ls "file:///" {:attrs true})
; => ({:url      "file:///Applications"
;      :dir      true
;      :modified #inst"2018-03-05T21:48:27.000-00:00"
;      :owner    "root"
;      :group    "admin"
;      :perms    "rwxrwxr-x"}
;     {:url      "file:///Library"
;      :dir      true
;      :modified #inst"2017-11-07T21:54:12.000-00:00"
;      :owner    "root"
;      :group    "wheel"
;      :perms    "rwxr-xr-x"}
;     ...)

; NOTE: by default, `ls` returns the minimum required set of attributes: :url, :size/:dir, :error.
; NOTE: extra attributes depend on actual FS and may not be present in other FS.

; Copying a large file from one URL to another:
(uio/copy "hdfs:///path/to/file.txt"
          "s3://bucket/key/with/slashes.txt")

; Defining credentials for multiple fs and paths
(uio/with {"s3://"                  {:access ...            ; default credentials for all S3 buckets
                                     :secret ...}

           "s3://bucket-a/"         {:access ...            ; credentials for bucket `bucket-a`
                                     :secret ...}

           "hdfs://"                {}                      ; default credentials for HDFS => use `kinit`

           "hdfs://site-a"          {:principal "guest"     ; use `guest` account to access `site-a`
                                     :keytab    "file:///path/to/guest.keytab"}

           "hdfs://site-a/home/joe" {:principal "joe"       ; however, override access to a specific folder for `joe`
                                     :keytab    "file:///path/to/joe.keytab"} }

  ; TODO put your code here
  )
```

See [Implementation-specific details](#implementation-specific-details) for the full list of configuration keys
and corresponding environment variables.

### Adding your FS
```clojure
(ns myns
  (:require [uio.uio :refer [from to size delete mkdir ls
                             with from* to* copy]))

; Implement the following methods in your namespace:
(defmethod from    :myftp [url & args] :TODO) ; should return an InputStream
(defmethod to      :myftp [url & args] :TODO) ; should return an OutputStream
(defmethod size    :myftp [url & args] :TODO) ; should return a Number
(defmethod exists? :myftp [url & args] :TODO) ; should return a boolean
(defmethod delete  :myftp [url & args] :TODO) ; should return nil
(defmethod mkdir   :myftp [url & args] :TODO) ; should return nil
(defmethod ls      :myftp [url & args] :TODO) ; should return a list of maps

; Use your implementation
(exists? "myftp://host/path/to/file.txt")

; NOTE: `copy` uses `from` and `to`, so there's no need in implementing it, except
;       special cases when either implementation of `from` or `to` can't be provided.
;       For an example, see implementation of `copy` for S3.
```

### Adding your codec
```clojure
(ns example
  (:require [uio.uio :refer [ext->is->is ext->os->os])
  (:import [java.io InputStream OutputStream]
           [java.util.zip GZIPInputStream GZIPOutputStream]))

; Implement the following methods in your namespace:
(defmethod ext->is->is :myzip [_] (fn [^InputStream is]  (GZIPInputStream. is)))
(defmethod ext->os->os :myzip [_] (fn [^OutputStream os] (GZIPOutputStream. os)))

; Use your codec
(spit  (to*   "file:///path/to/file.txt.myzip") "<content>")        ; => nil
(slurp (from* "file:///path/to/file.txt.myzip"))                    ; => "<content>"

; Combine with other codecs if needed
(spit (to* "file:///path/to/file.txt.myzip.bz2") "<content>")       ; => nil
```

## FS-specific details

### File
Your local file system, e.g. `file:///home/user`.

### HDFS
```clojure
(uio/with {"hdfs://" {:principal "joe"
                      :keytab    "file:///path/to/eytab"}}
  ...)

; NOTE: to use Kerberos (`kinit`) authentication, set to empty map: "hdfs://" {}.

; NOTE: to disable HDFS and remove all dependencies, add this exclusion to your `project.clj`:
:dependencies [[uio/uio "1.1" :exclusions [org.apache.hadoop/hadoop-common
                                           org.apache.hadoop/hadoop-hdfs]]]
```

### HTTP(S)
```clojure
; NOTE: HTTP implementation of `size` sends a HEAD request and expects
;       the server to set `content-length` header in response
;
(uio/size "https://github.com/apple-touch-icon-180x180.png")
; => 21128

(uio/size "http://www.google.com/")
; => clojure.lang.ExceptionInfo: Couldn't get size: header `content-length`
;    is not set {:url "http://www.google.com/"}

; NOTE: HTTP implementation of `exists?` sends a HEAD request and returns:
;       - true  -- if the server replies with 2XX status code
;       - false -- if the server replies with 404 status code
;       - throws exceptions in all other cases (IO errors, permissions etc.)
;
(uio/exists? "http://www.google.com")
; => true

(uio/exists? "http://www.google.com/asdf")
; => false

(uio/exists? "http://www.google.co")
; => clojure.lang.ExceptionInfo: Got non-2XX and non 404 status code
;    in repsonse from server {:url "http://www.google.co", :code 301}

(uio/exists? "http://www.google.cop")
; => java.net.UnknownHostException: www.google.cop
```

### Mem
```clojure
; In-memory filesystem. Useful for unit testing.

(ns myns
  (:require [uio.uio :as uio]
            [uio.fs.mem :as mem))

(mem/reset)                                                  ; deletes all files from memory

(mkdir "mem:///path")
(mkdir "mem:///path/to")

(spit  (uio/to*   "mem:///path/to/file.txt.gz") "<content>") ; => nil
(slurp (uio/from* "mem:///path/to/file.txt.gz"))             ; => "<content>"
```

### Res
```clojure
; Provides access to resources in classpath. Only `from` is implemented.

(slurp (uio/from "res:///uio/uio.clj"))                      ; ...source code of uio.clj as a String

```
```java
// the (uio/from ...) part in the code above is equivalent to Java code:
InputStream is = clojure.java.api.Clojure.class.getResourceAsStream("/uio/uio.clj");
```

### S3
```clojure
(uio/with {"s3://" {:access ...
                    :secret ...}}
  ...)

; NOTE: S3 doesn't have directories: `ls` will simulate directories in output, `mkdir` will do nothing.
```

### SFTP
```clojure
(uio/with {"sftp://" {:user          ...
                      :known-hosts   ...    ; <-- actual content
                      :pass          ...
                      :identity      ...    ; <-- actual content
                      :identity-pass ... }  ; optional
  ...)

; NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa -p <port> <host>`
;       and copy the content (skip the line starting with a #).

; NOTE: either :pass or :identity should be present.
```

## Command line tool

### Building
To build `uio` command from source, you will need:
- [JDK8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [git](https://git-scm.com/downloads)
- [lein](https://leiningen.org/)

Then run:
```
$ git clone git@github.com:oshyshko/uio.git
...

$ cd uio

$ ./scripts/build.sh
...
Created /Users/John/uio/target/uio-1.1.jar
Created /Users/John/uio/target/uio.jar
Creating standalone executable: /Users/John/uio/target/uio

$ cp ./target/uio ~/bin            <-- copy `uio` binary to a directory that is in your PATH (e.g. ~/bin)
```
### Configuration, usage and examples
Run once without args to create a default config in `~/.uio/config.clj`
```
$ uio
Expected a command, but got none.
To see examples, run `uio --help`.
```

Let's see some examples:
```
$ uio --help
Usage: cat file.txt | uio to       fs:///path/to/file.txt
       cat file.txt | uio to*      fs:///path/to/file.txt.gz

                      uio from     fs:///path/to/file.txt    > file.txt
                      uio from*    fs:///path/to/file.txt.gz > file.txt

                      uio size     fs:///path/to/file.txt
                      uio exists?  fs:///path/to/file.txt
                      uio delete   fs:///path/to/file.txt
                      uio mkdir    fs:///path/to/dir/

                      uio copy     fs:///source/path/to/file.txt fs:///destination/path/to/file.txt

                      uio ls [-lh] fs:///path/to/dir/
                              -l - list in long format (show attributes)
                              -h - print sizes in human readable format

                      uio --help - print this help

Common flags:                 -v - print stack traces and annoying logs to stderr

Experimental (will change in future!):
                      uio ls [-rs] fs:///path/to/dir/
                              -r - list files and directories recursively
                              -s - print total file size, file and directory count


Version: [uio/uio "1.1"]
FS:      file hdfs http https mem res s3 sftp
Codecs:  bz2 gz xz
Config:  file:///Users/john/.uio/config.clj
```

Let's see what's in the config.

```
$ cat ~/.uio/config.clj
```

```clojure
{
 ; For `kinit` authentication, use:
 ; "hdfs://" {}
 ;
 ; For keytab principal + path authentication, use:
 ; "hdfs://" {:principal "joe"
 ;            :keytab    "file:///path/to/principal.keytab"}
 ;
 ;
 "hdfs://" {}

 ; To use credentials from `~/.s3cfg`, add:
 ; "s3://" {}
 ;
 ; To use access/secret pair, add:
 ; "s3://"   {:access "access-key"
 ;            :secret "secret-key"}
 ;
 ;
 "s3://"   {}

 ; "sftp://" {:user          "joe"
 ;            :pass          "secret"                         ; optional
 ;            :known-hosts   "<ssh-rsa ...>"                  ; actual content (ssh-rsa)
 ;            :identity      "<identity-value>"               ; optional, actual content
 ;            :identity-pass "<identity-password-value>" }    ; optional, password for identity (if needed)
 ;
 ; NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa [-p <port>] <host>`
 ;       and copy the content (skip the line starting with a #).
 ;
 "sftp://" {:user        ""
            :known-hosts ""
            :pass        ""}

 ; NOTE: see also "Defining credentials for multiple fs and paths" at https://github.com/oshyshko/uio
 }
```
If you're already using `hadoop` or `hdfs` command together with `kinit`, or `s3cmd` command alone,
`uio` should work out of the box. The the default config will make `uio` use `kinit` for HDFS
and try to load configs from `~/.s3cfg`.

You can also configure `uio` to use credentials based on URL prefix.
For example you can leave `kinit` as default auth mechanism for all HDFS URLs
by adding `"hdfs://" {}` entry, and then override access to a specific clusters or paths with
URL prefix like `"hdfs://my-cluster/my-path/" {...}`.

This will also work for S3 buckets/paths and SSH hosts/ports/paths.

You can override multiple URL prefixes, the rule of thumb is: the longest URL prefix that matches your URL wins.

## License
Copyright © Oleksandr Shyshko. All rights reserved.

The use and distribution terms for this software are covered by the
Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
which can be found in the file LICENSE at the root of this distribution.

By using this software in any fashion, you are agreeing to be bound by the terms of this license.
You must not remove this notice, or any other, from this software.
