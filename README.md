`[uio/uio "1.0"]`

# uio

`uio` is a Clojure/Java library for accessing HDFS, S3, SFTP and other file systems via a single API.

Features:
- uses URLs everywhere (there are no relative paths or current working directories)
- built around 7 core functions, no global state, no def-macros
- can automatically encode/decode files based on their filename extension
- allows adding new protocols and codecs from REPL or user code

### Built-in protocols

|                  |from | to  |size |exists?|delete|mkdir| ls  | URL format                             |
|------------------|:---:|:---:|:---:|:-----:|:----:|:---:|:---:|----------------------------------------|
|[file](#file)     |  •  |  •  |  •  |   •   |  •   |  •  |  •  |`file:///path/to/file.txt`              |
|[hdfs](#hdfs)     |  •  |  •  |  •  |   •   |  •   |  •  |  •  |`hdfs://[host]/path/to/file.txt`        |
|[http(s)](#https) |  •  |     |:cat:| :cat: |      |     |     |`http[s]://host[:port]/path/to/file.txt`|
|[mem](#mem)       |  •  |  •  |  •  |   •   |  •   |:dog:|  •  |`mem:///path/to/file.txt`               |
|[res](#res)       |  •  |     |     |       |      |     |     |`res:///com/mypackage/file.txt`         |
|[s3](#s3)         |  •  |  •  |  •  |   •   |  •   |:dog:|  •  |`s3://bucket/key/with/slashes.txt`      |
|[sftp](#sftp)     |  •  |:bug:|  •  |   •   |:pig: |  •  |  •  |`sftp://host[:port]/path/to/file.txt`   |

- :cat: - limited support, see [HTTP(S)](#https)
- :dog: - these FS don't have directories: `ls` will emulate directories, `mkdir` will do nothing
- :bug: - the outbound content will be accumulated in a temporary `.gz` file and will be uploaded via SFTP when the stream is closed (a workaround to a concurrency bug in JSch)
- :pig: - for files only (won't delete directories)

Built-in codecs: `.bz2`, `.gz`, `.xz`.


### Example

For Java examples, see [Example.java](test/uio/Example.java).

```clojure
(ns example
  (:require [uio.uio :refer [from to size delete mkdir ls copy
                             from* to* with]))

; Reading and writing
(slurp (from "hdfs:///path/to/file.txt"))                   ; => "<content>"
(spit  (to "s3://bucket/key/with/slashes.txt") "<content>") ; => nil

; NOTE: `slurp` reads entire content into memory and it is only suitable for small files.

; NOTE: Streams returned by `from` and `to` should be closed by the caller (you).
;       Fns `slurp` and `split` will call `(.close ...)` automatically.
;       For other cases, it's highly recommended to use `with-open` macro
;       (it also calls `(.close ...)` on streams passed to it).
;       This approach will save your code from resource leaks or data corruption.

; Read the first line of some HTML
(with-open [is (from "http://www.google.com/")]
  (->> (line-seq (clojure.java.io/reader is))
       (take 1)
       (doall)))
; => ("<!doctype ...")

; Write an image to a file
(with-open [os (to "file:////path/to/green-dot.gif")]
  (.write os (byte-array [0x47 0x49 0x46 0x38 0x37 0x61 0x01 0x00
                          0x01 0x00 0x80 0x00 0x00 0x00 0xd6 0x7e
                          0x00 0x00 0x00 0x2c 0x00 0x00 0x00 0x00
                          0x01 0x00 0x01 0x00 0x00 0x02 0x02 0x44
                          0x01 0x00 0x3b])))
; => nil

; Reading and writing with extension codecs
(slurp (from* "file:///path/to.gz")                         ; decompress with `gzip`
(spit  (to*   "file:///path/to.bz2") "<content>")           ; compress with `bzip2`
(spit  (to*   "file:///path/to.xz.bz2.gz") "<content>")     ; compress with `xz`, 'bzip2' and `gzip`

; Getting file size
(size "file:///path/to/file.txt")                           ; => Number

; Testing a file for existence
(exists? "file:///path/to/file.txt")                        ; => boolean

; Deleting individual files and directories
(delete "file:///path/to/file.txt")                         ; => nil
(delete "file:///path/to")                                  ; => nil

; Listing directory contents
(->> (ls "s3://bucket/path/to")                             ; returns a lazy sequence. Potentially,
     (take 4))                                              ; ... a very large one, thus, using
                                                            ; ... (take ...) is not a bad idea.
                                                            ;
; => [{:url     "s3://bucket/path/to/some-dir"              ; a dir
;      :dir     true}
;     {:url     "s3://bucket/path/to/file1.txt"             ; a file (has :size)
;      :size    123}
;     {:url     "s3://bucket/path/to/file2.txt"             ; a symlink to a file
;      :symlink true
;      :size    456}]
;     {:url     "s3://bucket/path/to/dir"                   ; a symlink to a dir
;      :symlink true}]

; Combine with `filter` and `map` to get the output you want
(->> (ls "s3://bucket/path/to/")
     (remove :dir)                                          ; remove dirs
     (map :url)                                             ; leave URLs only
     (take 3))
; => ["s3://bucket/path/to/file1.txt"
;     "s3://bucket/path/to/file2.txt"
;     "s3://bucket/path/to/file3.txt"]

; Listing files recursively
(->> (ls "s3://bucket/path/to/" {:recurse true})            ; override default value
     (remove :dir)                                          ; leave files only
     (map :size)                                            ; leave sizes only
     (reduce +))                                            ; get total size (in bytes)
; => 12345678

; Copying a large file from one URL to another:
(copy "hdfs:///path/to/file.txt"
      "s3://bucket/key/with/slashes.txt")

; Defining or overriding existing configuration (e.g. credentials)
(with {:s3.access "..."
       :s3.secret "..."}
  (slurp (from "s3://aaa/key/with/slashes.txt")))

; Configurations can be nested
(with {:s3.access "AAA"
       :s3.secret "BBB"}

  (slurp (from "s3://aaa/key/with/slashes.txt"))            ; use AAA/BBB

  (with {:s3.access "XXX"                                   ; override :s3.access and :s3.secret for
         :s3.secret "YYY"}
    (slurp (from "s3://xxx/key/with/slashes.txt")))         ; access with XXX/YYY credentials

  (slurp (from "s3://aaa/key/with/slashes.txt")))           ; access with AAA/BBB credentials

; Also, default values for :s3.access and :s3.secret can be set via environment
; variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
;
; NOTE: `(with ...)` will always override configuration set via environment variables.
;
```
See [Implementation-specific details](#implementation-specific-details) for the full list of configuration keys
and corresponding environment variables.

### Adding your implementation
```clojure
(ns myns
  (:require [uio.uio :refer [from to size delete mkdir ls
                             with from* to* copy]))

; Implement the following methods in your namespace:
(defmethod from    :myftp [url]        :TODO) ; should return an InputStream
(defmethod to      :myftp [url]        :TODO) ; should return an OutputStream
(defmethod size    :myftp [url]        :TODO) ; should return a Number
(defmethod exists? :myftp [url]        :TODO) ; should return a boolean
(defmethod delete  :myftp [url]        :TODO) ; should return nil
(defmethod mkdir   :myftp [url]        :TODO) ; should return nil
(defmethod ls      :myftp [url & opts] :TODO) ; should return a list of maps

; Use your implementation
(exists? "myftp://host/path/to/file.txt")

; NOTE: `copy` uses `from` and `to`, so there's no need in implementing it, except
;       special cases when either implementation of `from` or `to` can't be provided.
;       For an example, see implementation of `copy` for S3.
```

### Adding your filename extension codec
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

## Implementation-specific details

### File
Your local file system, e.g. `file:///home/user`.

### HDFS
```clojure
(with {:hdfs.keytab.principal ...     ; HDFS_KEYTAB_PRINCIPAL              (optional)
       :hdfs.keytab.path      ...     ; HDFS_KEYTAB_PATH                   (optional) <-- path to a file
       :s3.access             ...     ; AWS_ACCESS / AWS_ACCESS_KEY_ID     (optional)
       :s3.secret             ...}    ; AWS_SECRET / AWS_SECRET_ACCESS_KEY (optional)
  ...)

; NOTE: to use Kerberos (`kinit`) authentication, leave empty values in
;       :hdfs.keytab.principal (KEYTAB_PRINCIPAL) and :hdfs.keytab.path (KEYTAB_FILE).
```

### HTTP(S)
```clojure
; NOTE: HTTP implementation of `size` sends a HEAD request and expects
;       the server to set `content-length` header in response
;
(size "https://github.com/apple-touch-icon-180x180.png")
; => 21128

(size "http://www.google.com/")
; => clojure.lang.ExceptionInfo: Couldn't get size: header `content-length`
;    is not set {:url "http://www.google.com/"}

; NOTE: HTTP implementation of `exists?` sends a HEAD request and returns:
;       - true  -- if the server replies with 2XX status code
;       - false -- if the server replies with 404 status code
;       - throws exceptions in all other cases (IO errors, permissions etc.)
;
(exists? "http://www.google.com")
; => true

(exists? "http://www.google.com/asdf")
; => false

(exists? "http://www.google.co")
; => clojure.lang.ExceptionInfo: Got non-2XX and non 404 status code
;    in repsonse from server {:url "http://www.google.co", :code 301}

(exists? "http://www.google.cop")
; => java.net.UnknownHostException: www.google.cop
```

### Mem
```clojure
; An in-memory filesystem. Useful for unit testing.

(ns myns
  (:require [uio.uio :refer [from* to*]]
            [uio.fs.mem :as mem))

(mem/reset)                                                 ; deletes all files from memory

(spit (to* "mem:///path/to/file.txt.gz") "<content>")       ; => nil
(slurp (from* "mem:///path/to/file.txt.gz"))                ; => "<content>"
```

### Res
```clojure
; Provides access to files in classpath. Only `from` is implemented.

(slurp (from "res:///uio/uio.clj"))                         ; ...source code of uio.clj as a String

```
```java
// the (from ...) part in the code above is equivalent to Java code:
InputStream is = clojure.java.api.Clojure.class.getResourceAsStream("/uio/uio.clj");
```

### S3
```clojure
(with {:s3.access ...                 ; AWS_ACCESS / AWS_ACCESS_KEY_ID
       :s3.secret ...}                ; AWS_SECRET / AWS_SECRET_ACCESS_KEY
  ...)

; NOTE: S3 doesn't have directories: `ls` will simulate directories in output, `mkdir` will do nothing.
```

### SFTP
```clojure
(with {:sftp.user          ...  ; SFTP_USER
       :sftp.pass          .... ; SFTP_PASS
       :sftp.identity      ...  ; SFTP_IDENTITY       <-- actual content
       :sftp.identity.pass ...  ; SFTP_IDENTITY_PASS  (optional)
       :sftp.known-hosts   ...} ; SFTP_KNOWN_HOSTS    <-- actual content
  ...)

; NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa -p <port> <host>`
;      and copy the content (skip the line starting with a #).

; NOTE: either :sftp.pass (SFTP_PASS) or :sftp.identity (SFTP_IDENTITY) should be present.
```

## Building

```bash
$ ./scripts/install.sh
```

## License

The use and distribution terms for this software are covered by the
Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
which can be found in the file LICENSE at the root of this distribution.

By using this software in any fashion, you are agreeing to be bound by the terms of this license.
You must not remove this notice, or any other, from this software.
