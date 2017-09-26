(ns uio.main
  (:gen-class)
  (:require [uio.uio :as uio]
            [uio.impl :refer [die list-available-implementations]]
            [clojure.string :as str]
            [clojure.java.io :as jio])
  (:import [org.apache.log4j Level Logger]))

(defn err [& msg]
  (binding [*out* *err*]
    (apply print msg)
    (flush)))

(defn errln [& msg]
  (binding [*out* *err*]
    (apply println msg)))

; TODO .         -> file://<cwd>/
; TODO file.txt  -> file://<cwd>/file.txt
; TODO /file.txt -> file:///file.txt
;
; TODO?    (copy "file.txt" "hdfs:///path/to/")
; TODO? => (copy "file.txt" "hdfs:///path/to/file.txt")
;

(defn run [op url args]
  (case op
    "from"    (with-open [is (uio/from url)]  (jio/copy is System/out :buffer-size 8192))
    "to"      (with-open [os (uio/to   url)]  (jio/copy System/in os  :buffer-size 8192))
    "from*"   (with-open [is (uio/from* url)] (jio/copy is System/out :buffer-size 8192))
    "to*"     (with-open [os (uio/to*   url)] (jio/copy System/in os  :buffer-size 8192))
    "size"    (println (uio/size url))
    "exists?" (if-not (uio/exists? url) (die "exit 1"))
    "delete"  (uio/delete url)
    "mkdir"   (uio/mkdir url)
    "ls"      (doseq [f (uio/ls url (if (= "-r" (first args))
                                  {:recurse true}))] ; TODO move to first position
                (println (if (:size f)
                           (format "%10d" (:size f))
                           "       DIR")
                         (:url f)))

    "copy"    (uio/copy url (first args)) ; TODO check url-b

    (do (errln)
        (errln "Usage:   cat file.txt | uio to      hdfs:///path/to/file.txt")
        (errln "         cat file.txt | uio to*     hdfs:///path/to/file.txt.gz")
        (errln)
        (errln "                        uio from    hdfs:///path/to/file.txt    > file.txt")
        (errln "                        uio from*   hdfs:///path/to/file.txt.gz > file.txt")
        (errln)
        (errln "                        uio size    hdfs:///path/to/file.txt")
        (errln "                        uio exists? hdfs:///path/to/file.txt")
        (errln "                        uio delete  hdfs:///path/to/file.txt")
        (errln "                        uio mkdir   hdfs:///path/to/file.txt")
        (errln "                        uio ls      hdfs:///path/to")
        (errln "                        uio ls   -r hdfs:///path/to")
        (errln)
        (errln "FS:     " (str/join " " (map name (:fs     (list-available-implementations)))))
        (errln "Codecs: " (str/join " " (map name (:codecs (list-available-implementations)))))
        (errln)
        (errln "Version:" (str "["
                               (try (->> (uio/from "res:///META-INF/leiningen/uio/uio/project.clj")
                                         slurp
                                         (re-find #"^\(defproject (.+\")")
                                         second)
                                    (catch Throwable _ "unknown"))
                               "]"))
        (errln))))

(defn try-s3cmd-or-nil []
  (try (->> (str/split (slurp (str (System/getProperty "user.home") "/" ".s3cfg")) #"\n")
            (remove #(#{\# \;} (first %)))
            (map #(let [[k v] (str/split % #"\s?=\s?" 2)] ; TODO does take into account comments. Use clojure-ini?
                    [(keyword k) v]))
            (into {})
            ((fn [m]
               {:s3.access (or (:access_key m) (die "Can't find :access_key in ~/.s3cfg. Skipping"))
                :s3.secret (or (:secret_key m) (die "Can't find :secret_key in ~/.s3cfg. Skipping"))})))
       (catch Exception _ nil)))

(defn -main [& [op url & args]]
  (.setLevel (Logger/getRoot) Level/OFF)
  ; TODO ensure `url` and `url-b` are URLs
  (try
    (uio/with (merge {}
                 (try-s3cmd-or-nil))

      (run op url args))

    (catch Throwable e (when-not (= "exit 1" (.getMessage e))
                         (errln (str e))
                         (errln)
                         (errln e))
                       (System/exit 1))

    (finally (System/exit 0))))
