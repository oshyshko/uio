(ns uio.fs.hdfs
  "HDFS -- hdfs://host/path/to/file.txt
           hdfs:///path/to/file.txt

   :hdfs.keytab.principal  --OR-- env HDFS_KEYTAB_PRINCIPAL              (optional)
   :hdfs.keytab.path       --OR-- env HDFS_KEYTAB_PATH                   (optional) <-- path to a file
   :s3.access              --OR-- env AWS_ACCESS / AWS_ACCESS_KEY_ID     (optional)
   :s3.secret              --OR-- env AWS_SECRET / AWS_SECRET_ACCESS_KEY (optional)
  "
  (:require [clojure.string :as str]
            [uio.impl :refer :all])
  (:import [java.io IOException]
           [java.net URL]
           [java.util Iterator]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileAlreadyExistsException FileStatus FileSystem Path RemoteIterator]
           [org.apache.hadoop.security UserGroupInformation]))

(deftype HdfsIterator [^FileSystem fs ^RemoteIterator ri] Iterator
  (hasNext  [_] (.hasNext ri))
  (next     [_] (.next ri)))

(defn ->config []
  (let [c            (Configuration.)
        nil-if-empty (fn [s] (if (str/blank? s) nil s))

        keytab-prin  (nil-if-empty (or (config :hdfs.keytab.principal) (env "HDFS_KEYTAB_PRINCIPAL") (env "KEYTAB_PRINCIPAL")))
        keytab-path  (nil-if-empty (or (config :hdfs.keytab.path)      (env "HDFS_KEYTAB_PATH")      (env "KEYTAB_FILE")))
        aws-access   (nil-if-empty (or (config :s3.access)             (env "AWS_ACCESS")            (env "AWS_ACCESS_KEY_ID")))
        aws-secret   (nil-if-empty (or (config :s3.secret)             (env "AWS_SECRET")            (env "AWS_SECRET_ACCESS_KEY")))]

    (when (and aws-access aws-secret)
      (.set c "fs.s3a.impl"               "org.apache.hadoop.fs.s3a.S3AFileSystem")
      (.set c "fs.s3a.access.key"         aws-access)
      (.set c "fs.s3a.secret.key"         aws-secret)

      (.set c "fs.s3n.impl"               "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      (.set c "fs.s3n.awsAccessKeyId"     aws-access)
      (.set c "fs.s3n.awsSecretAccessKey" aws-secret)

      (.set c "fs.s3.impl"                "org.apache.hadoop.fs.s3.S3FileSystem")
      (.set c "fs.s3.awsAccessKeyId"      aws-access)
      (.set c "fs.s3.awsSecretAccessKey"  aws-secret))

    (doseq [url ["file:///etc/hadoop/conf/core-site.xml"
                 "file:///etc/hadoop/conf/hdfs-site.xml"]]
      (if (exists? url)
        (.addResource c (URL. url))))
    
    (.set c "hadoop.security.authentication" "kerberos")

    (UserGroupInformation/setConfiguration c)

    ; only use keytab creds if either user or keytab path was specified, otherwise rely on default auth (e.g. if ran from kinit/Yarn)
    (when (or keytab-prin keytab-path)
      (UserGroupInformation/loginUserFromKeytab keytab-prin keytab-path)

      ; TODO is there a way to provide more information about the failure?
      (if-not (UserGroupInformation/isLoginKeytabBased)
        (die "Could not authenticate. Wrong or missing keytab?")))

    c))

(defn with-hdfs [fs->x]
  (with-open [fs (FileSystem/newInstance (->config))]
    (fs->x fs)))

(defn file-status->kv [^FileStatus f]
  (cond (.isFile f)      {:url (-> f .getPath .toUri str) :size (.getLen f)}
        (.isDirectory f) {:url (-> f .getPath .toUri str) :dir true}
        :else            (die (str "Don't know what to do with " (-> f .getPath .toUri)
                                   " -- it's neither a file, nor a dir.") {:f f}))

  ; TODO uncomment in future, when have better understanding of overlap with other FS
  #_(merge {:url              (-> f .getPath .toUri str)
          :hdfs.accessed    (-> f .getAccessTime Date.)
          :hdfs.modified    (-> f .getModificationTime Date.)
          :hdfs.user        (-> f .getOwner)
          :hdfs.group       (-> f .getGroup)
          :hdfs.permissions (-> f .getPermission str)}

         (if (.isFile f)      {:size             (-> f .getLen)
                               :hdfs.replication (-> f .getReplication)
                               :hdfs.block-size  (-> f .getBlockSize)})

         (if (.isDirectory f) {:dir              true})
         (if (.isSymlink f)   {:hdfs.symlink     (-> f .getSymlink .toUri str)})
         (if (.isEncrypted f) {:hdfs.encrypted   true})))

(defmethod from    :hdfs [url]        (wrap-is #(FileSystem/newInstance (->config))
                                               #(.open % (Path. (->url url)))
                                               #(.close %)))

(defmethod to      :hdfs [url]        (wrap-os #(FileSystem/newInstance (->config))
                                               #(.create % (Path. (->url url)))
                                               #(.close %)))

(defmethod exists? :hdfs [url]        (with-hdfs #(.exists % (Path. (->url url)))))
(defmethod size    :hdfs [url]        (with-hdfs #(.getLen (.getFileStatus % (Path. (->url url))))))

(defmethod delete  :hdfs [url]        (with-hdfs
                                        #(do (and (not (.delete % (Path. (->url url))))
                                                  (.exists % (Path. (->url url)))
                                                  (die "Could not delete: got `false` and the file still exists" {:url url}))
                                             nil)))

(defmethod ls      :hdfs [url & opts] (let [opts (get-opts default-opts-ls url opts)]
                                        (let [fs (FileSystem/newInstance (->config))]
                                          (cond->>
                                            (->> (if (:recurse opts) ; RemoteIterator<LocatedFileStatus>
                                                   (.listFiles fs (Path. (->url url)) true)
                                                   (.listStatusIterator fs (Path. (->url url))))
                                                 (HdfsIterator. fs) ; Iterator<LocatedFileStatus>
                                                 (iterator-seq) ; [FileStatus]
                                                 (map file-status->kv) ; [{kv}]
                                                 (close-when-realized-or-finalized #(.close fs)))
                                            (:recurse opts) (intercalate-with-dirs)))))

(defmethod mkdir   :hdfs [url & opts] (with-hdfs #(do (or (try (.mkdirs % (Path. (->url url)))
                                                               (catch FileAlreadyExistsException _
                                                                 (die "A file with this name already exists" {:url url})))
                                                          (try (.isDirectory (.getFileStatus % (Path. (->url url))))
                                                               (catch IOException e
                                                                 (die "Could not make directory: " {:url url} e)))
                                                          (die "A file with this name already exists" {:url url}))
                                                      nil)))
