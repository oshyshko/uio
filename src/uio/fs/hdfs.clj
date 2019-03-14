; HDFS
;
; hdfs://host/path/to/file.txt
; hdfs:///path/to/file.txt
;
;  :principal       (optional)
;  :keytab          (optional)  <-- path to a file
;  :access          (optional)  S3 access
;  :secret          (optional)  S3 secret
;
;  NOTE: to use `kinit` isntead of keytab file, pass empty creds (`{}` or all nil values)
;
(ns uio.fs.hdfs
  (:require [clojure.string :as str]
            [uio.fs.file]
            [uio.impl :refer :all])
  (:import [java.io IOException]
           [java.net URL]
           [java.util Iterator Date]
           [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FileAlreadyExistsException FileStatus FileSystem Path RemoteIterator]
           [org.apache.hadoop.security UserGroupInformation]))

(deftype HdfsIterator [^FileSystem fs ^RemoteIterator ri] Iterator
  (hasNext  [_] (.hasNext ri))
  (next     [_] (.next ri)))

(defn ->config [^String url]
  (let [c           (Configuration.)
        creds       (url->creds url)

        principal   (:principal creds)
        keytab-path (some-> (:keytab creds) path)
        aws-access  (:access creds)
        aws-secret  (:secret creds)]

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
    (when (or principal keytab-path)
      (UserGroupInformation/loginUserFromKeytab principal keytab-path)

      ; TODO is there a way to provide more information about the failure?
      (if-not (UserGroupInformation/isLoginKeytabBased)
        (die "Could not authenticate. Wrong or missing keytab?")))

    c))

(defn ->fs [^String url]
  (FileSystem/newInstance (->URI url)
                          (->config url)))

(defn with-hdfs [^String url fs->x]
  (with-open [fs (->fs url)]
    (fs->x fs)))

(defmethod from    :hdfs [url & args] (wrap-is #(->fs url)
                                               #(.open % (Path. (->URI url)))
                                               #(.close %)))

(defmethod to      :hdfs [url & args] (wrap-os #(->fs url)
                                               #(.create % (Path. (->URI url)))
                                               #(.close %)))

(defmethod exists? :hdfs [url & args] (with-hdfs url #(.exists % (Path. (->URI url)))))
(defmethod delete  :hdfs [url & args] (with-hdfs url #(let [opts (get-opts default-opts-delete url args)]
                                                        (and (not (.delete % (Path. (->URI url)) (:recurse opts)))
                                                             (.exists % (Path. (->URI url)))
                                                             (die (str "Could not delete: got `false` and the file still exists: " url) ))
                                                        nil)))

(defmethod mkdir   :hdfs [url & args] (with-hdfs url #(do (or (try (.mkdirs % (Path. (->URI url)))
                                                                   (catch FileAlreadyExistsException _
                                                                     (die (str "A file with this name already exists: " url))))
                                                              (try (.isDirectory (.getFileStatus % (Path. (->URI url))))
                                                                   (catch IOException e
                                                                     (die (str "Could not make directory: " url) e)))
                                                              (die (str "A file with this name already exists: " url)))
                                                          nil)))

(defn fix-hdfs-url [url]
  (str/replace url #"\+" "%2B"))

(defn f->attrs [extended? ^FileStatus f]
  (merge {:url ((if (.isDirectory f)
                  ensure-ends-with-delimiter
                  ensure-not-ends-with-delimiter)
                 (fix-hdfs-url (str (.toUri (.getPath f)))))}

         (if (.isFile f)      {:size (.getLen f)})
         (if (.isDirectory f) {:dir  true})

         (if extended?
           (merge {:accessed (-> f .getAccessTime Date.)
                   :modified (-> f .getModificationTime Date.)
                   :owner    (-> f .getOwner)
                   :group    (-> f .getGroup)
                   :perms    (str (.getPermission f))}

                  (if (.isSymlink f)   {:symlink     (-> f .getSymlink .toUri str fix-hdfs-url)})
                  (if (.isEncrypted f) {:encrypted   true})
                  (if (.isFile f)      {:replication (-> f .getReplication)
                                        :block-size  (-> f .getBlockSize)})))))

(defmethod attrs   :hdfs [url & [opts]]
  (with-hdfs url #(f->attrs true (.getFileStatus % (Path. (->URI url))))))

(defmethod ls      :hdfs [url & args] (single-file-or
                                        url
                                        (let [opts (get-opts default-opts-ls url args)
                                              fs   (->fs url)
                                              p    (Path. (->URI url))]
                                          (cond->> (->> (if (or (str/includes? url "?")
                                                                (str/includes? url "*"))
                                                          (.globStatus fs p)
                                                          (->> (if (:recurse opts) ; RemoteIterator<LocatedFileStatus>
                                                                 (.listFiles fs p true)
                                                                 (.listStatusIterator fs p))
                                                               (HdfsIterator. fs) ; Iterator<LocatedFileStatus>
                                                               iterator-seq)) ; [FileStatus]
                                                        (map (partial f->attrs (:attrs opts))) ; [{kv}]
                                                        (close-when-realized-or-finalized #(.close fs)))

                                                   ; TODO fix a case for a tree of dirs w/o files
                                                   (:recurse opts)
                                                   (intercalate-with-dirs url)))))

(defmethod move :hdfs [from-url to-url & args]
  ; TODO assert both URLs are on the same cluster (host part of URLs are equal)
  (with-hdfs from-url
             (fn [fs]
               (when-not (.rename fs
                                  (Path. (->URI from-url))
                                  (Path. (->URI to-url)))
                 (when-not (exists? from-url)
                   (die-no-such-file from-url))
                 (when (exists? from-url))
                 (die (str "Couldn't move " (pr-str from-url) " to " (pr-str to-url)))))))


(defn get-usage [url]
  (with-hdfs url
             (fn [^FileSystem fs]
               (bean (.getContentSummary fs (Path. (->URI url)))))))
