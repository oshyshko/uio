(ns uio.fs.https
  "HTTP(S) -- http(s)://host[:port]/path/to/file.txt"
  (:require [uio.impl :refer :all])
  (:import [java.net URL]))

(defn http-https-size [url]
  (let [c (.openConnection (URL. url))]
    (try (.setRequestMethod c "HEAD")
         (if-not (<= 200 (.getResponseCode c) 299)
           (die "Couldn't get size: got non-2XX status code in repsonse from server"
                {:url url :code (.getResponseCode c)}))

         (if-not (.getHeaderField c "content-length")
           (die "Couldn't get size: header `content-length` was not set"
                {:url url :code (.getResponseCode c)}))

         (.getContentLength c)
         (finally (.disconnect c)))))


(defn http-https-exists? [url]
  (let [c (.openConnection (URL. url))]
    (try (.setRequestMethod c "HEAD")
         (cond (<= 200 (.getResponseCode c) 299) true
               (=  404 (.getResponseCode c))     false
               :else
               (die "Got non-2XX and non 404 status code in repsonse from server"
                    {:url  url :code (.getResponseCode c)}))

         (finally (.disconnect c)))))

(defmethod from    :http  [url & args] (.openStream (URL. url)))
(defmethod size    :http  [url & args] (http-https-size url))
(defmethod exists? :http  [url & args] (http-https-exists? url))
; `to` and `delete` are not implemented for HTTP

(defmethod from    :https [url & args] (.openStream (URL. url)))
(defmethod size    :https [url & args] (http-https-size url))
(defmethod exists? :https [url & args] (http-https-exists? url))
; `to` and `delete` are not implemented for HTTPS
