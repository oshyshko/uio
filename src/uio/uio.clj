(ns uio.uio
  (:require [uio.impl :as i]))

; public API
(def from                 i/from)
(def to                   i/to)
(def size                 i/size)
(def exists?              i/exists?)
(def delete               i/delete)
(def ls                   i/ls)
(def mkdir                i/mkdir)
(def copy                 i/copy)
(def from*                i/from*)
(def to*                  i/to*)
(def ext->is->is          i/ext->is->is)
(def ext->os->os          i/ext->os->os)

(def with-fn              i/with-fn)

; TODO resolve the clash with i/with
(defmacro with [config & body]
  `(with-fn ~config (fn [] ~@body)))

; URL manipulation
(def ->url                i/->url)
(def normalize            i/normalize)
(def scheme               i/scheme)
(def host                 i/host)
(def port                 i/port)
(def path                 i/path)
(def path-no-slash        i/path-no-slash)
(def filename             i/filename)
(def encode-url           i/encode-url)

(def with-parent          i/with-parent)
(def parent-of            i/parent-of)
(def ends-with-delimiter? i/ends-with-delimiter?)

(def default-delimiter    i/default-delimiter)

; stream helper fns
(def is->bytes            i/is->bytes)
(def bytes->is            i/bytes->is)
(def with-baos->bytes     i/with-baos->bytes)

(def ->nil-os             i/->nil-os)
(def ->countable          i/->countable)                    ; use with (count ...)
(def ->digestible         i/->digestible)                   ; use with (close-and-digest ...)
(def close-and-digest     i/close-and-digest)

; TODO consider replacing the above with something like:
; (defmacro pullall [ns]
;   `(do ~@(for [[sym var] (ns-publics ns)]
;            `(def ~sym ~var))))

; Implementations: fs
(require 'uio.fs.file)
(require 'uio.fs.hdfs)
(require 'uio.fs.https)
(require 'uio.fs.mem)
(require 'uio.fs.res)
(require 'uio.fs.s3)
(require 'uio.fs.sftp)

; Implementations: codecs
(require 'uio.codecs.bz2)
(require 'uio.codecs.gz)
(require 'uio.codecs.xz)
