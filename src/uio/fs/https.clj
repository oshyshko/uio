; HTTP(S)
;
; http(s)://host[:port]/path/to/file.txt
;
(ns uio.fs.https
  (:require [uio.impl :refer :all])
  (:import [java.net URL]))

(defn http-https-size [url]
  (let [c (.openConnection (URL. url))]
    (try (.setRequestMethod c "HEAD")
         (if-not (<= 200 (.getResponseCode c) 299)
           (die (str "Couldn't get size: got non-2XX status code " (.getResponseCode c) " in response for URL: " url)))

         (if-not (.getHeaderField c "content-length")
           (die (str "Couldn't get size: header `content-length` was not set, code " (.getResponseCode c) " for URL: " url)))

         (.getContentLength c)
         (finally (.disconnect c)))))


(defn http-https-exists? [url]
  (let [c (.openConnection (URL. url))]
    (try (.setRequestMethod c "HEAD")
         (cond (<= 200 (.getResponseCode c) 299) true
               (=  404 (.getResponseCode c))     false
               :else
               (die (str "Got non-2XX and non-404 status code " (.getResponseCode c) " in response for URL: " url)))

         (finally (.disconnect c)))))

(defmethod from    :http  [url & args] (.openStream (URL. url)))
(defmethod size    :http  [url & args] (http-https-size url))
(defmethod exists? :http  [url & args] (http-https-exists? url))
; `to` and `delete` are not implemented for HTTP

(defmethod from    :https [url & args] (.openStream (URL. url)))
(defmethod size    :https [url & args] (http-https-size url))
(defmethod exists? :https [url & args] (http-https-exists? url))
; `to` and `delete` are not implemented for HTTPS
