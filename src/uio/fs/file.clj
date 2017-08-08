(ns uio.fs.file
  "Local FS -- file:///path/to/file.txt
                    ^^^ triple slash
  "
  (:require [uio.impl :refer :all])
  (:import [java.io File]
           [java.nio.file Files Paths OpenOption LinkOption AccessDeniedException NoSuchFileException]
           [java.nio.file.attribute FileAttribute]
           [java.util.stream Stream]))

(deftype Finalizer [^Stream s] Object
  (finalize [_] (.close s)))

(defmethod from    :file [url] (-> url ->url Paths/get (Files/newInputStream    (into-array OpenOption []))))
(defmethod to      :file [url] (-> url ->url Paths/get (Files/newOutputStream   (into-array OpenOption []))))
(defmethod size    :file [url] (-> url ->url Paths/get (Files/size)))
(defmethod exists? :file [url] (-> url ->url Paths/get (Files/exists            (into-array LinkOption []))))
(defmethod delete  :file [url] (-> url ->url Paths/get (Files/deleteIfExists)))
(defmethod mkdir   :file [url] (-> url ->url Paths/get (Files/createDirectories (into-array FileAttribute []))))

(defmethod ls      :file [url & [opts]]
    (lazy-cat ; create a recursive fn and kick it
              ((fn -ls [url]
                 (try
                   (let [stream (-> url ->url Paths/get Files/list)
                         z      (->Finalizer stream)]
                     (lazy-cat (mapcat #(let [is-symlink  (Files/isSymbolicLink %)
                                              is-dir      (Files/isDirectory % (into-array LinkOption []))
                                              file-url (str (.toUri %))]
                                          (cons
                                            ; next element
                                            (merge {:url (ensure-has-no-trailing-slash file-url)}

                                                   (if is-symlink
                                                     {:symlink (ensure-has-no-trailing-slash (str (.toUri (Files/readSymbolicLink %))))})

                                                   (if is-dir
                                                     {:dir true}
                                                     (try {:size (Files/size %)}
                                                          (catch NoSuchFileException e {:size 0 :error (str e)})))) ; TODO improve error reporting?
                                            ; recurse if asked
                                            (if (and is-dir
                                                     (:recurse opts)
                                                     (and (not is-symlink)))
                                              (lazy-seq (-ls file-url)))))

                                       (iterator-seq (.iterator stream)))

                               ; called upon reaching end of the `concat`ed list
                               (.finalize z)))

                   (catch AccessDeniedException e [{:url url :error (str e)}]) ; TODO improve error reporting?
                   (catch Exception e             (die "Couldn't list files for" {:url url} e))))

                 ; apply the fn with starting dir
                (.replaceAll url "/+$" ""))))

; TODO consider removing or moving elsewhere
(defn path->url   ^String [^String path]  (str (.toURI (File. path))))

(defn ->temp-file ^String [prefix suffix] (str (.toUri (Files/createTempFile      prefix suffix (into-array FileAttribute [])))))
(defn ->temp-dir  ^String [prefix]        (str (.toUri (Files/createTempDirectory prefix        (into-array FileAttribute [])))))
