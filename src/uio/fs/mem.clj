; Mem
;
; mem://path/to/file.txt
;
(ns uio.fs.mem
  (:require [uio.impl :refer :all]
            [clojure.string :as str])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

; byte[] = file, nil = directory; files ends with non-/, dirs end with /
(def *url->bytes-or-nil (atom nil))

(def decoy (Object.))

(defn reset []
  (reset! *url->bytes-or-nil {})
  nil)

(reset)

; TODO implement offset + length
(defmethod from    :mem [url & args] (let [url (normalize url)]
                                       (ByteArrayInputStream.
                                         (if (exists? url)
                                           (or (@*url->bytes-or-nil url)
                                               (die-not-a-file url))
                                           (die-no-such-file url)))))

(defmethod to      :mem [url & args] (let [url (normalize url)]
                                       (when-not (exists? (parent-of url))
                                         (die-parent-not-found (parent-of url)))

                                       (when (and (exists? url)
                                                  (:dir (attrs url)))
                                         (die-dir-already-exists url))

                                       (wrap-os #(ByteArrayOutputStream.)
                                                identity
                                                #(swap! *url->bytes-or-nil
                                                        assoc
                                                        url
                                                        (.toByteArray %)))))

(defmethod size    :mem [url & args] (or (some->> (@*url->bytes-or-nil (normalize url))
                                                  count
                                                  long)
                                         (die-no-such-file (normalize url))))

(defmethod exists? :mem [url & args] (or (= default-delimiter (path url)) ; roots for any FS always exist
                                         (contains? @*url->bytes-or-nil (ensure-not-ends-with-delimiter (normalize url)))
                                         (contains? @*url->bytes-or-nil (normalize url))))

(defmethod delete  :mem [url & [op]] (let [url (normalize url)]
                                       (if (and (:recurse op)
                                                (:dir  (attrs url)))
                                         (swap! *url->bytes-or-nil
                                                (fn [old-url->byte-os-nil]
                                                  (->> old-url->byte-os-nil
                                                       keys
                                                       (filter #(str/starts-with? (ensure-ends-with-delimiter %)
                                                                                  (ensure-ends-with-delimiter url)))
                                                       (reduce (fn [m k]
                                                                 (dissoc m (ensure-not-ends-with-delimiter k)))
                                                               old-url->byte-os-nil))))
                                         
                                         (do (cond (:dir  (attrs url)) (when (not-empty (ls url)) (die-dir-not-empty url))
                                                   (:size (attrs url)) (when (ends-with-delimiter? url) (die-not-a-dir url))
                                                   :else               (die-should-never-reach-here (str "got something that's neither file, nor dir: " (pr-str))))

                                             (swap! *url->bytes-or-nil
                                                    dissoc
                                                    (ensure-not-ends-with-delimiter url)))))

                                       nil)

(defmethod ls      :mem [url & [op]] (single-file-or
                                       url
                                       (let [url (ensure-ends-with-delimiter (normalize url))]
                                         (->> @*url->bytes-or-nil
                                              (filter (fn [[k _]] (and (str/starts-with? k url)
                                                                       (not= url k)
                                                                       (or (:recurse op)
                                                                           (= url (parent-of k))))))
                                              (map #(attrs (first %)))
                                              (sort-by :url)))))

(defmethod attrs   :mem [url & args] (let [url         (normalize url)
                                           url-as-file (ensure-not-ends-with-delimiter url)
                                           bs          (get @*url->bytes-or-nil url-as-file decoy)
                                           bs          (cond (= default-delimiter (path url))    nil ; it's a root dir
                                                             (identical? decoy bs)               (die-no-such-file url)
                                                             (and bs (ends-with-delimiter? url)) (die-not-a-dir url-as-file)
                                                             :else                               bs)] ; it's either dir or file
                                       (if bs
                                         {:url url :size (long (count bs))}
                                         {:url (ensure-ends-with-delimiter url) :dir true})))

(defmethod mkdir   :mem [url & args] (let [url (normalize url)]
                                       (when (exists? url)
                                         (if (:dir (attrs url))
                                           nil              ; do nothing
                                           (die-file-already-exists (ensure-not-ends-with-delimiter url))))
                                       (mkdirs-up-to url #(swap! *url->bytes-or-nil
                                                                 assoc
                                                                 (ensure-not-ends-with-delimiter %)
                                                                 nil))))
