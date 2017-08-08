(ns uio.fs.mem
  (:require [uio.impl :refer :all]
            [clojure.string :as str])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def ^:private *url->bytes (atom (sorted-map)))

(defn reset []
  (reset! *url->bytes (sorted-map)))

(defmethod from    :mem [url]        (ByteArrayInputStream. (or (@*url->bytes url)
                                                                (die "File not found" {:url url}))))

(defmethod to      :mem [url]        (wrap-os #(ByteArrayOutputStream.)
                                              identity
                                              #(swap! *url->bytes assoc url (.toByteArray %))))

(defmethod size    :mem [url]        (or (some->> (@*url->bytes url)
                                                  (count))
                                         (die "File not found" {:url url})))

(defmethod exists? :mem [url]        (some? (@*url->bytes url)))

(defmethod delete  :mem [url]        (do (swap! *url->bytes dissoc url) nil))

(defmethod ls      :mem [url & args] (let [opts     (get-opts default-opts-ls url args)
                                           recurse  (:recurse opts)
                                           base-dir (ensure-ends-with-delimiter url)]

                                       (->> @*url->bytes
                                            (filter (fn [[k _]] (str/starts-with? k base-dir)))

                                            (map (fn [[k v]]
                                                   (let [next-delimiter-i (str/index-of k default-delimiter (count base-dir))]
                                                     (if (or recurse
                                                             (not next-delimiter-i))
                                                       {:url k                           :size (count v)}
                                                       {:url (subs k 0 next-delimiter-i) :dir  true}))))

                                            (distinct)

                                            (intercalate-with-dirs)

                                            ((if recurse identity rest)))))

(defmethod mkdir   :mem [url & args] (do :nothing nil))
