(ns uio.fs.file
  "Local FS -- file:///path/to/file.txt
                    ^^^ triple slash
  "
  (:require [uio.impl :refer :all])
  (:import [java.io File]
           [java.nio.file Files Paths OpenOption LinkOption Path]
           [java.nio.file.attribute FileAttribute PosixFileAttributes PosixFilePermissions]
           [java.util Date]))

; TODO implement :offset + :length + assert all args are known
(defmethod from    :file [url & args]   (-> url ->url Paths/get (Files/newInputStream    (into-array OpenOption []))))
(defmethod to      :file [url & args]   (-> url ->url Paths/get (Files/newOutputStream   (into-array OpenOption []))))
(defmethod size    :file [url & args]   (-> url ->url Paths/get (Files/size)))
(defmethod exists? :file [url & args]   (-> url ->url Paths/get (Files/exists            (into-array LinkOption []))))
(defmethod delete  :file [url & args]   (-> url ->url Paths/get (Files/deleteIfExists)))
(defmethod mkdir   :file [url & args]   (-> url ->url Paths/get (Files/createDirectories (into-array FileAttribute []))))

; TODO assert all args are known
(defmethod attrs   :file [url & [opts]] (Files/setPosixFilePermissions (Paths/get (->url url))
                                                                                  (PosixFilePermissions/fromString (:perms opts))))

(defn f->kv [file-url attrs? is-dir is-symlink ^Path f]
  (try
    (merge {:url file-url}

           (if is-dir
             {:dir true}
             {:size (Files/size f)})

           (if attrs?
             (let [attrs (Files/readAttributes f PosixFileAttributes (into-array LinkOption []))]
               (merge {:modified (Date. (.toMillis (.lastModifiedTime attrs)))
                       :owner    (-> attrs .owner .getName)
                       :group    (-> attrs .group .getName)
                       :perms    (str (if is-dir "d" "-")
                                      (PosixFilePermissions/toString (.permissions attrs)))}
                      (if is-symlink
                        {:symlink (->> (Files/readSymbolicLink f) ; resolve absolute + relative link
                                       (.resolve (.resolveSibling f "."))
                                       (.normalize)
                                       (#(str "file://" % (if is-dir default-delimiter))))})))))

    (catch Exception e {:url file-url :error (str e)})))

(defn -ls [url recurse? attrs?]
  (try
    (let [s (-> url ->url Paths/get Files/list)]
      (->> (iterator-seq (.iterator s))
           (sort-by #(.getFileName %))
           (mapcat #(let [is-symlink (Files/isSymbolicLink %)
                          is-dir     (Files/isDirectory % (into-array LinkOption []))
                          file-url   (str (.toUri %))]      ; already ends with /
                      (cons (f->kv file-url attrs? is-dir is-symlink %)
                            (if (and is-dir
                                     recurse?
                                     (and (not is-symlink)))
                              (lazy-seq (-ls file-url recurse? attrs?))))))

           (close-when-realized-or-finalized #(.close s))))

    (catch Exception e [{:url url :error (str e)}])))

(defmethod ls      :file [url & args] (let [opts (get-opts default-opts-ls url args)]
                                        (-ls (normalize url)
                                               (:recurse opts)
                                               (:attrs opts))))

; TODO consider removing or moving elsewhere
(defn path->url   ^String [^String path]  (str (.toURI (File. path))))

(defn ->temp-file ^String [prefix suffix] (str (.toUri (Files/createTempFile      prefix suffix (into-array FileAttribute [])))))
(defn ->temp-dir  ^String [prefix]        (str (.toUri (Files/createTempDirectory prefix        (into-array FileAttribute [])))))
