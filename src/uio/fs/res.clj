(ns uio.fs.res
  "Classpath FS -- res:///path/to/file.txt
                       ^^^ triple slash
  "
  (:require [uio.impl :refer [die from host path ]])
  (:import (clojure.java.api Clojure)))

(defmethod from :res [url] (do (if (host url)
                                 (die "A resource should have no host e.g. (three slashes expected like 'res:///path/to/file.txt')" {:url url}))
                               (or (.getResourceAsStream Clojure (path url))
                                   (die (str "Couldn't open input stream from a resource. "
                                             "It probably doesn't exist or belongs to another classloader")
                                        {:url url}))))
