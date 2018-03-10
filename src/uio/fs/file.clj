; Local FS
;
; file:///path/to/file.txt
;      ^^^ triple slash
;
(ns uio.fs.file
  (:require [uio.impl :refer :all])
  (:import [java.io File]
           [java.nio.file Files Paths OpenOption LinkOption]
           [java.nio.file.attribute FileAttribute PosixFileAttributes PosixFilePermissions]
           [java.util Date]))

; TODO implement :offset + :length + assert all args are known
(defmethod from    :file [url & args]   (-> url ->URI Paths/get (Files/newInputStream    (into-array OpenOption []))))
(defmethod to      :file [url & args]   (-> url ->URI Paths/get (Files/newOutputStream   (into-array OpenOption []))))
(defmethod exists? :file [url & args]   (-> url ->URI Paths/get (Files/exists            (into-array LinkOption []))))
(defmethod delete  :file [url & args]   (-> url ->URI Paths/get (Files/deleteIfExists)))
(defmethod mkdir   :file [url & args]   (-> url ->URI Paths/get (Files/createDirectories (into-array FileAttribute []))))

; TODO assert all args are known
(defmethod attrs :file [url & [opts]]
  ; TODO implement :owner :group :modified :accessed
  (when (:perms opts)
    (Files/setPosixFilePermissions (Paths/get (->URI url))
                                   (PosixFilePermissions/fromString (:perms opts))))
  ; TODO implement :accessed
  (let [p          (Paths/get (->URI url))
        is-symlink (Files/isSymbolicLink p)
        is-dir     (Files/isDirectory p (into-array LinkOption []))
        as         (Files/readAttributes p PosixFileAttributes (into-array LinkOption []))]

    (merge {:url url}

           (if is-dir
             {:dir true}
             {:size (Files/size p)})

           {:modified (Date. (.toMillis (.lastModifiedTime as)))
            :owner    (-> as .owner .getName)
            :group    (-> as .group .getName)
            :perms    (PosixFilePermissions/toString (.permissions as))}

           (if is-symlink
             {:symlink (->> (Files/readSymbolicLink p)      ; resolve absolute + relative link
                            (.resolve (.resolveSibling p "."))
                            (.normalize)
                            (#(str "file://" % (if is-dir default-delimiter))))}))))

(defn -ls [url recurse? attrs?]
  (try
    (let [s (-> url ->URI Paths/get Files/list)]
      (->> (iterator-seq (.iterator s))
           (sort-by #(.getFileName %))
           (mapcat #(let [url (replace-path "file:///" (str %))
                          as  (try (attrs (replace-path "file:///" (str %)))
                                   (catch Exception e {:url url :error e}))]
                      (cons (if attrs?
                              as
                              (select-keys as minimal-attrs))
                            (if (and (:dir as)
                                     recurse?
                                     (and (not (:symlink as))))
                              (lazy-seq (-ls (:url as) recurse? attrs?))))))

           (close-when-realized-or-finalized #(.close s))))

    (catch Exception e [{:url url :error e}])))

(defmethod ls      :file [url & args] (single-file-or
                                        url
                                        (let [opts (get-opts default-opts-ls url args)]
                                          (-ls (normalize url)
                                               (:recurse opts)
                                               (:attrs opts)))))

; TODO consider removing or moving elsewhere
(defn path->url   ^String [^String path]  (str (.toURI (File. path))))

(defn ->temp-file ^String [prefix suffix] (str (.toUri (Files/createTempFile      prefix suffix (into-array FileAttribute [])))))
(defn ->temp-dir  ^String [prefix]        (str (.toUri (Files/createTempDirectory prefix        (into-array FileAttribute [])))))
