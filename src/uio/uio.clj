(ns uio.uio
  (:require [clojure.string :as str]
            [uio.impl :as i])
  (:import [uio.fs Streams]))

; public API
(def from                 i/from)
(def to                   i/to)
(def size                 i/size)
(def exists?              i/exists?)
(def delete               i/delete)
(def ls                   i/ls)
(def mkdir                i/mkdir)
(def attrs                i/attrs)
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

; codec helper fns
(def decode               i/decode)
(def encode               i/encode)

(def ->nil-os             i/->nil-os)
(def ->countable          i/->countable)                    ; use with (count ...)
(def ->digestible         i/->digestible)                   ; use with (close-and-digest ...)
(def close-and-digest     i/close-and-digest)

; TODO consider replacing the above with something like:
; (defmacro pullall [ns]
;   `(do ~@(for [[sym var] (ns-publics ns)]
;            `(def ~sym ~var))))

; TODO report the error instead of disabling an impl: when called, tell what classes are missing + give a hint about :exclusions
; TODO check if namespace depends in `uio.uio` and fail if it does
(defn require-if-all-deps-are-in-cp [ns]
  (let [class-available? #(try (Class/forName % false (.getClassLoader Streams))
                               true
                               (catch ClassNotFoundException _ false))
        resource-path    (str "/" (str/replace ns "." "/") ".clj")
        s-exp            (clojure.edn/read-string (slurp (.getResourceAsStream Streams resource-path)))
        dep-classes      (mapcat #(or (and (seq? %)
                                           (= :import (first %))
                                           (sort (mapcat (fn [[package & classes]] ; => ["java.io.IOException" "java.net.URL" ... ]
                                                           (for [c classes]
                                                             (str package "." c)))
                                                         (drop 1 %))))
                                      [])
                                 s-exp)]


    (when (every? class-available? dep-classes)
      (require ns))))

(doseq [ns '[uio.fs.file
             uio.fs.hdfs
             uio.fs.https
             uio.fs.mem
             uio.fs.res
             uio.fs.s3
             uio.fs.sftp
             uio.codecs.bz2
             uio.codecs.gz
             uio.codecs.xz]]
  (require-if-all-deps-are-in-cp ns))
