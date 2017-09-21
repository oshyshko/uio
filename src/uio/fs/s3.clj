(ns uio.fs.s3
  "S3 -- s3://bucket/path/to/file.txt

   :s3.access  --OR--  env AWS_ACCESS / AWS_ACCESS_KEY_ID
   :s3.secret  --OR--  env AWS_SECRET / AWS_SECRET_ACCESS_KEY
  "
  (:require [uio.impl :refer :all])
  (:import [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model ListObjectsRequest ObjectListing ObjectMetadata S3ObjectSummary]
           [com.amazonaws.services.s3.transfer TransferManager]
           [uio.fs S3$S3OutputStream]))

(defn bucket-key->url [b k]
  (str "s3://" b default-delimiter k))

(defn ^BasicAWSCredentials ->creds []
  (BasicAWSCredentials. (or (config :s3.access)
                            (env "AWS_ACCESS")
                            (env "AWS_ACCESS_KEY_ID")
                            (die "Either (uio/with {:s3.access ...} ... ), or AWS_ACCESS / AWS_ACCESS_KEY_ID env expected to be set"))

                        (or (config :s3.secret)
                            (env "AWS_SECRET")
                            (env "AWS_SECRET_ACCESS_KEY")
                            (die "Either (uio/with {:s3.secret ...} ... ), or AWS_SECRET / AWS_SECRET_ACCESS_KEY env expected to be set"))))

(defn with-s3 [url client-bucket-key->x]
  (try-with #(AmazonS3Client. (->creds))
            #(client-bucket-key->x % (host url) (path-no-slash url))
            #(.shutdown %)))

(defmethod from    :s3 [url]        (wrap-is #(AmazonS3Client. (->creds))
                                             #(.getObjectContent (.getObject % (host url) (path-no-slash url)))
                                             #(.shutdown %)))

(defmethod to      :s3 [url]        (wrap-os #(AmazonS3Client. (->creds))
                                             #(S3$S3OutputStream. % (host url) (path-no-slash url))
                                             #(.shutdown %)))

(defmethod exists? :s3 [url]        (with-s3 url (fn [c b k] (.doesObjectExist c b k))))
(defmethod size    :s3 [url]        (with-s3 url (fn [c b k] (.getContentLength (.getObjectMetadata c b k)))))
(defmethod delete  :s3 [url]        (with-s3 url (fn [c b k] (.deleteObject c b k))))

(defmethod mkdir   :s3 [url & args] (do :nothing nil))      ; S3 doesn't support directories

(defmethod copy    :s3 [from-url to-url]
  (try-with #(TransferManager. (->creds))
            #(with-open [is (from from-url)]
               (.waitForCompletion (.upload %
                                            (host to-url)
                                            (path-no-slash to-url)
                                            is
                                            (doto (ObjectMetadata.)
                                                  (.setContentLength (size from-url))))))
            #(.shutdownNow %)))

(defn -ls [delimiter recurse url marker]
  (with-s3 url
           (fn [c b k]
             (let [^ObjectListing ol (.listObjects c (ListObjectsRequest. b k marker (if recurse nil delimiter) nil))] ; ObjectListing
               (concat
                 ; this page
                 (sort-by :url                              ; move dirs between files. S3 returns 1000 files per batch + dirs, so it's ok to sort in memory
                          (concat
                            ; files
                            (for [^S3ObjectSummary s (.getObjectSummaries ol)]
                              {:url              (bucket-key->url (.getBucketName s) (.getKey s))
                               :size             (-> s .getSize)
                               ;; S3-specific attributes -- TODO uncomment in future, when have better understanding of overlap with other FS
                               ;:s3.modified      (-> s .getLastModified)
                               ;:s3.etag          (-> s .getETag)
                               ;:s3.owner         (-> s .getOwner .getDisplayName)
                               ;:s3.owner-id      (-> s .getOwner .getId)
                               ;:s3.storage-class (-> s .getStorageClass)
                               })
                            ; dirs
                            (for [^String s (.getCommonPrefixes ol)]
                              {:url              (bucket-key->url b (subs s 0 (dec (.length s))))
                               :dir              true})))
                 ; next page
                 (if (.isTruncated ol)
                   (lazy-seq (-ls delimiter
                                  recurse
                                  url
                                  (.getNextMarker ol)))))))))

(defmethod ls      :s3 [url & args] (let [opts (get-opts default-opts-ls url args)]
                                      (cond->> (-ls default-delimiter
                                                    (:recurse opts)
                                                    (ensure-ends-with-delimiter url)
                                                    nil)
                                               (:recurse opts) (intercalate-with-dirs))))
