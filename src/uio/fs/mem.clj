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

(defn reset []
  (reset! *url->bytes-or-nil (sorted-map "mem:///" nil)))

(reset)

; TODO implement offset + length
(defmethod from    :mem [url & args] (ByteArrayInputStream.
                                       (if (exists? url)
                                         (or (@*url->bytes-or-nil url)
                                             (die-dir-already-exists url))
                                         (die-file-not-found url))))

(defmethod to      :mem [url & args] (when-not (exists? (parent-of url))
                                       (die-parent-not-found (parent-of url)))
                                     (when (and (exists? url)
                                                (:dir (attrs url)))
                                       (die-dir-already-exists url))
                                     (wrap-os #(ByteArrayOutputStream.)
                                              identity
                                              #(swap! *url->bytes-or-nil assoc url (.toByteArray %))))

(defmethod size    :mem [url & args] (or (some->> (@*url->bytes-or-nil url)
                                                  (count))
                                         (die-file-not-found url)))

(defmethod exists? :mem [url & args] (do (or (contains? @*url->bytes-or-nil (ensure-not-ends-with-delimiter url))
                                             (contains? @*url->bytes-or-nil (ensure-ends-with-delimiter url)))))

(defmethod delete  :mem [url & args] (do (swap! *url->bytes-or-nil dissoc url)
                                         nil))

(defmethod ls      :mem [url & [op]] (do (single-file-or
                                           url
                                           (let [base-dir       (ensure-ends-with-delimiter (normalize url))
                                                 base-dir-key   (ensure-not-ends-with-delimiter base-dir)
                                                 decoy          (Object.)
                                                 base-dir-value (get @*url->bytes-or-nil base-dir-key decoy)]
                                             (cond (identical? decoy base-dir-value) (die-file-not-found url)
                                                   (some? base-dir-value)            (die-not-a-dir base-dir-key))
                                             (->> @*url->bytes-or-nil
                                                  (filter (fn [[k _]] (and (str/starts-with? k base-dir)
                                                                           (not= base-dir k)
                                                                           (or (:recurse op)
                                                                               (= base-dir (parent-of k))))))
                                                  (map #(attrs (first %)))
                                                  (sort-by :url))))))

(defmethod attrs   :mem [url & args] (if (contains? @*url->bytes-or-nil (ensure-not-ends-with-delimiter url))
                                       (let [bs (@*url->bytes-or-nil url)]
                                         (if bs
                                           {:url url :size (count bs)}
                                           {:url (ensure-ends-with-delimiter url) :dir true}))
                                       (die-file-not-found url)))

(defmethod mkdir   :mem [url & args] (do (when (exists? url)
                                           (if (:dir (attrs url))
                                             (die-dir-already-exists (ensure-ends-with-delimiter url))
                                             (die-file-already-exists (ensure-not-ends-with-delimiter url))))
                                         (swap! *url->bytes-or-nil assoc (ensure-not-ends-with-delimiter url) nil)
                                         nil))
