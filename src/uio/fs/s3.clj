; S3
;
; s3://bucket/path/to/file.txt
;
; :access
; :secret
; :role-arn
;
(ns uio.fs.s3
  (:require [uio.impl :refer :all]
            [clojure.string :as str])
  (:import [com.amazonaws.auth BasicAWSCredentials STSAssumeRoleSessionCredentialsProvider AWSCredentialsProvider]
           [com.amazonaws.internal StaticCredentialsProvider]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model ListObjectsRequest ObjectListing S3ObjectSummary GetObjectRequest CannedAccessControlList GetObjectTaggingRequest]
           [uio.fs S3$S3OutputStream]))

(defn bucket-key->url [b k]
  (str "s3://" b default-delimiter k))

(defn path-no-leading-slash [^String url]
  (subs (or (path url) "?") 1))

(defn ^AWSCredentialsProvider ->creds-provider [url]
  (let [{:keys [access secret role-arn] :as creds} (url->creds url)
        _     (if-not access (die-creds-key-not-found :access url creds))
        _     (if-not secret (die-creds-key-not-found :secret url creds))
        bawsc (BasicAWSCredentials. access secret)]
    (if role-arn
      (STSAssumeRoleSessionCredentialsProvider. bawsc ^String role-arn "uio-s3-session")
      (StaticCredentialsProvider. bawsc))))

(defn with-client-bucket-key [url c-b-k->x]
  (try-with url
            #(AmazonS3Client. (->creds-provider url))
            #(c-b-k->x % (host url) (path-no-leading-slash url))
            #(.shutdown %)))

; See https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html?shortFooter=true#canned-acl
(defn acl->enum [^String s]
  (let [m (->> (CannedAccessControlList/values)
               (map #(vector (str %) %))
               (into {}))]
    (or (m s)
        (die (str "Couldn't find canned ACL " (pr-str s) ". Available options are: " (str/join ", " (map pr-str (sort (keys m)))))))))

(defmethod from    :s3 [url & args] (let [opts  (get-opts default-opts-from url args)
                                          start (or (:offset opts) 0)
                                          end   (if (:length opts)
                                                  (+ start
                                                     (:length opts))
                                                  (dec (Long/MAX_VALUE)))]
                                      (wrap-is #(AmazonS3Client. (->creds-provider url))
                                               #(.getObjectContent
                                                  (.getObject %
                                                              (.withRange
                                                                (GetObjectRequest. (host url) (path-no-leading-slash url))
                                                                start
                                                                end)))
                                               #(.shutdown %))))

(defmethod to      :s3 [url & [opts]] (wrap-os #(AmazonS3Client. (->creds-provider url))
                                               #(S3$S3OutputStream. % (host url) (path-no-leading-slash url) (some-> opts :acl acl->enum))
                                               #(.shutdown %)))

(defmethod exists? :s3 [url & args] (with-client-bucket-key url (fn [c b k] (.doesObjectExist c b k))))
(defmethod size    :s3 [url & args] (with-client-bucket-key url (fn [c b k] (.getContentLength (.getObjectMetadata c b k)))))
(defmethod delete  :s3 [url & args] (with-client-bucket-key url (fn [c b k] (.deleteObject c b k))))

(defmethod mkdir   :s3 [url & args] (do :nothing nil))      ; S3 doesn't support directories

(defn -ls [c b k url recurse? attrs? delimiter marker]
  (let [^ObjectListing l (.listObjects c (ListObjectsRequest. b k marker (if recurse? nil delimiter) nil))]
    (concat
      ; this page
      (sort-by :url                                      ; move dirs between files. S3 returns 1000 files per batch + dirs, so it's ok to sort in memory
               (concat
                 ; files
                 (for [^S3ObjectSummary s (.getObjectSummaries l)]
                   (cond->  {:url  (bucket-key->url (.getBucketName s) (.getKey s))
                             :size (.getSize s)}
                            attrs?
                            (merge {:modified (.getLastModified s)})))
                 ; dirs
                 (for [^String s (.getCommonPrefixes l)]
                   {:url (cond-> (bucket-key->url b s)
                                 recurse?
                                 (ensure-ends-with-delimiter))
                    :dir true})))

      ; next page
      (if (.isTruncated l)
        (lazy-seq (-ls c
                       b
                       k
                       url
                       recurse?
                       attrs?
                       delimiter
                       (.getNextMarker l)))))))

(defmethod attrs   :s3 [url & args] (with-client-bucket-key url
                                                            (fn [c b k]
                                                              (if-let [s (first (.getObjectSummaries (.listObjects c (ListObjectsRequest. b k nil nil nil))))]
                                                                (cond
                                                                  ; a file?
                                                                  (= (path-no-leading-slash url)
                                                                     (.getKey s))
                                                                  {:url url :size (.getSize s)}

                                                                  ; a dir or sub-directory? (doesn't have files)
                                                                  :else
                                                                  (loop [url-asked (ensure-ends-with-delimiter url)
                                                                         url-found (parent-of (replace-path url (str default-delimiter (.getKey s))))]
                                                                    (cond (not (str/starts-with? url-found url-asked))
                                                                          (die-file-not-found url)

                                                                          (= url-asked url-found)
                                                                          {:url url-asked :dir true}

                                                                          :else
                                                                          (recur url-asked (parent-of url-found)))))

                                                                (die-file-not-found url)))))

(defmethod ls      :s3 [url & args] (single-file-or
                                      url
                                      (let [opts (get-opts default-opts-ls url args)
                                            c    (AmazonS3Client. (->creds-provider url))
                                            b    (host url)
                                            k    (path-no-leading-slash (ensure-ends-with-delimiter url))]
                                        (cond->> (close-when-realized-or-finalized
                                                   #(.shutdown c)
                                                   (-ls c
                                                        b
                                                        k
                                                        (ensure-ends-with-delimiter url)
                                                        (:recurse opts)
                                                        (:attrs opts)
                                                        default-delimiter
                                                        nil)) ; nil => list from beginning

                                                 (:recurse opts) (intercalate-with-dirs url)))))
