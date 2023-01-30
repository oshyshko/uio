; Resources in classpath
;
; res:///path/to/file.txt
;     ^^^ triple slash
;
(ns uio.fs.res
  (:require
    [uio.impl :refer :all])
  (:import (clojure.java.api Clojure)
           (java.io File)))

(defn assert-res-url [url]
  (if (host url)
    (die (str "Resource URL expected to have no host (e.g. 'res:///path/to/file.txt'), but it was: " (pr-str url)))
    url))

; TODO implement offset + length
(defmethod from    :res [url & args] (or (.getResourceAsStream Clojure (path (assert-res-url url)))
                                         (die (str "Couldn't open input stream from a resource. "
                                                   "It probably doesn't exist or belongs to another classloader: " url))))

(defmethod exists? :res [url & args] (if (.getResource Clojure (path (assert-res-url url)))
                                       true
                                       false))

(defmethod ls :res [url & args]
  (->>
    (.substring (path (normalize url)) 1)                   ; get path and remove leading slash
    (.getResources (.getClassLoader Clojure))               ; Multiple resources can have the same name
    (enumeration-seq)
    (map #(File. (.getPath %)))
    (map #(if (.isFile %)
            %
            (seq (.listFiles %))))
    (flatten)                                               ; If it's a directory, flatten the list of files.
    (map #(do {:url (str "file://" %) :size (.length %)}))))                  ; expected format is a list of maps
