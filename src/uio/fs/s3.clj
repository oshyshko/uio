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
  (:import [com.amazonaws.services.s3 AmazonS3ClientBuilder]
           [com.amazonaws.services.s3.model ListObjectsRequest ObjectListing S3ObjectSummary GetObjectRequest CannedAccessControlList AmazonS3Exception]
           [uio.fs S3$S3OutputStream]
           [java.nio.file NoSuchFileException]))

(defn bucket-key->url [b k]
  (str "s3://" b default-delimiter (escape-path k)))

(defn url->key [^String url]
  (subs (or (path url) "_") 1))

(defn client-for-url [^String url]
  (-> (AmazonS3ClientBuilder/standard)
      (.withForceGlobalBucketAccessEnabled true)
      (.build)))

(defn with-client-bucket-key [url c-b-k->x]
  (try-with url
            #(client-for-url url)
            #(c-b-k->x % (host url) (url->key url))
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
                                      (wrap-is #(client-for-url url)
                                               #(.getObjectContent
                                                  (.getObject %
                                                              (.withRange
                                                                (GetObjectRequest. (host url) (url->key url))
                                                                start
                                                                end)))
                                               #(.shutdown %))))

(defmethod to      :s3 [url & [opts]] (wrap-os #(client-for-url url)
                                               #(S3$S3OutputStream. % (host url) (url->key url) (some-> opts :acl acl->enum))
                                               #(.shutdown %)))

(defmethod exists? :s3 [url & args] (try (attrs url)
                                         true
                                         (catch NoSuchFileException _ false)))

(defmethod size    :s3 [url & args] (with-client-bucket-key url (fn [c b k] (.getContentLength (.getObjectMetadata c b k)))))

(defn -ls [c b k url recurse? attrs? delimiter marker]
  (let [^ObjectListing l (.listObjects c (ListObjectsRequest. b k marker (if recurse? nil delimiter) nil))]
    (concat
      ; this page
      (sort-by :url                                      ; move dirs between files. S3 returns 1000 files per batch + dirs, so it's ok to sort in memory
               (concat
                 ; files
                 (for [^S3ObjectSummary s (.getObjectSummaries l)]
                   (cond-> (merge {:url (bucket-key->url (.getBucketName s) (.getKey s))}
                                  (if (str/ends-with? (.getKey s) default-delimiter)
                                    {:dir true}
                                    {:size (.getSize s)}))

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

(defmethod delete  :s3 [url & args] (let [opts (get-opts default-opts-ls url args)]
                                      (with-client-bucket-key url
                                                              (fn [c b k]
                                                                (doseq [entry (if (:recurse opts)
                                                                                (-ls c
                                                                                     b
                                                                                     k
                                                                                     (ensure-ends-with-delimiter url)
                                                                                     true
                                                                                     false
                                                                                     default-delimiter
                                                                                     nil)
                                                                                [{:url url}])]
                                                                  (.deleteObject c b (url->key (:url entry))))))))

(defmethod mkdir   :s3 [url & args] (do :nothing nil))      ; S3 doesn't support directories

(defmethod attrs   :s3 [url & args] (with-client-bucket-key url
                                                            (fn [c b k]
                                                              (if-let [s (first (.getObjectSummaries (.listObjects c (ListObjectsRequest. b k nil nil nil))))]
                                                                (cond
                                                                  ; dir?
                                                                  (str/ends-with? (.getKey s) default-delimiter)
                                                                  {:url (replace-path url (str default-delimiter (.getKey s)))
                                                                   :dir true}

                                                                  ; file?
                                                                  (= (url->key url)
                                                                     (.getKey s))
                                                                  {:url url :size (.getSize s)}

                                                                  ; dir or sub-dir? (w/o files) TODO requires revision
                                                                  :else
                                                                  ; can it be a dir somewhere that is preceded file(s) with the same prefix?
                                                                  (if (not (str/ends-with? k default-delimiter))

                                                                    ; ...try add / to the end
                                                                    (recur c b (str k default-delimiter))

                                                                    ; other wise dig up and see if there's a matching parent
                                                                    (loop [url-asked (ensure-ends-with-delimiter url)
                                                                           url-found (parent-of (replace-path url (str default-delimiter (.getKey s))))]
                                                                      (cond (not (str/starts-with? url-found url-asked))
                                                                            (die-no-such-file url)

                                                                            (= url-asked url-found)
                                                                            {:url url-asked :dir true}

                                                                            :else
                                                                            (recur url-asked (parent-of url-found))))))

                                                                ; no files? die unless it's root
                                                                (if (= default-delimiter (path url))
                                                                  {:url url
                                                                   :dir true}
                                                                  (die-no-such-file url))))))

(defmethod ls      :s3 [url & args] (single-file-or
                                      url
                                      (let [opts (get-opts default-opts-ls url args)
                                            c    (client-for-url url)
                                            b    (host url)
                                            k    (url->key (ensure-ends-with-delimiter url))]
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

(defmethod copy    :s3 [from-url to-url & args] (if (and (= (host from-url)
                                                            (host to-url))
                                                         (= (url->creds from-url)
                                                            (url->creds to-url)))

                                                  (with-client-bucket-key from-url
                                                                          (fn [c _ _]
                                                                            (try
                                                                              (.copyObject c
                                                                                           (host from-url)
                                                                                           (url->key from-url)
                                                                                           (host to-url)
                                                                                           (url->key to-url))
                                                                              nil
                                                                              (catch AmazonS3Exception e
                                                                                (if (str/includes? (.getMessage e) "The specified copy source is larger than the maximum allowable size for a copy source")
                                                                                  (invoke-default copy from-url to-url)
                                                                                  (throw e))))))

                                                  (invoke-default copy from-url to-url)))
