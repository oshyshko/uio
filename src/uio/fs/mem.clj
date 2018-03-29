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
  (reset! *url->bytes-or-nil (sorted-map "mem:///" nil)))

(reset)

; TODO implement offset + length
(defmethod from    :mem [url & args] (let [url (normalize url)]
                                       (ByteArrayInputStream.
                                         (if (exists? url)
                                           (or (@*url->bytes-or-nil url)
                                               (die-dir-already-exists url))
                                           (die-file-not-found url)))))

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
                                                  (count))
                                         (die-file-not-found (normalize url))))

(defmethod exists? :mem [url & args] (or (contains? @*url->bytes-or-nil (ensure-not-ends-with-delimiter (normalize url)))
                                         (contains? @*url->bytes-or-nil (normalize url))))

(defmethod delete  :mem [url & args] (let [url (normalize url)]
                                       (cond (:dir  (attrs url)) (when (seq (ls url))             (die-dir-not-empty url))
                                             (:size (attrs url)) (when (ends-with-delimiter? url) (die-not-a-dir url))
                                             :else               (die (str "Should never reach here. Got something that's not a file, nor a dir: " (pr-str))))

                                       (swap! *url->bytes-or-nil
                                              dissoc
                                              (ensure-not-ends-with-delimiter url))
                                       nil))

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
                                           bs          (get @*url->bytes-or-nil url-as-file decoy)]

                                       (cond (identical? decoy bs)               (die-file-not-found url)
                                             (and bs (ends-with-delimiter? url)) (die-not-a-dir url-as-file))

                                       (if bs
                                         {:url url :size (count bs)}
                                         {:url (ensure-ends-with-delimiter url) :dir true})))

(defmethod mkdir   :mem [url & args] (let [url (normalize url)]
                                       (when (exists? url)
                                         (if (:dir (attrs url))
                                           (die-dir-already-exists (ensure-ends-with-delimiter url))
                                           (die-file-already-exists (ensure-not-ends-with-delimiter url))))
                                       (swap! *url->bytes-or-nil assoc
                                              (ensure-not-ends-with-delimiter url)
                                              nil)
                                       nil))
