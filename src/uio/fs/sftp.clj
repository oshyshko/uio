(ns uio.fs.sftp
  "SFTP -- sftp://host[:port]/path/to/file.txt

   :sftp.user              --OR--  env SFTP_USER
   :sftp.known-hosts       --OR--  env SFTP_KNOWN_HOSTS       <-- actual content (ssh-rsa)

     NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa [-p <port>] <host>`
           and copy the content (skip the line starting with a #).

   Either (1) or (2) should be present:
   1) :sftp.pass           --OR--  env SFTP_PASS

   2) :sftp.identity       --OR--  env SFTP_IDENTITY          <-- actual content
      :sftp.identity.pass  --OR--  env SFTP_IDENTITY_PASS     (optional)
  "
  (:require [clojure.string :as str]
            [uio.fs.file :as file]
            [uio.impl :refer :all])
  (:import [com.jcraft.jsch JSch ChannelSftp SftpException Session Channel]
           [java.io ByteArrayInputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]))

; see `(defmethod ls :sftp ...)` for details
(deftype Finalizer [^Session s ^Channel c] Object
  (finalize [_] (.disconnect c)
                (.disconnect s)))

(defn reformat-private-key-if-needed
  "JSch expects a private key with new-line characters as described in RFC-4716.
   However, it's usefull to pass private keys around as a single-line string where new-lines are replaced with space.
   This fn will convert a single-line private key back to multi-line format and make JSch happy."
  [s]
  (if (str/includes? s "\n")
    s
    (if-let [[_ header body footer] (re-find #"^(-+[^-]+-+)([^-]+)(-+[^-]+-+)" s)]
      (str header
           "\n"
           (->> (str/replace body #"\s" "")
                (partition-all 64)
                (map #(apply str %))
                (str/join "\n"))
           "\n"
           footer)
      (die "Got a private key without line separators, tried to reformat it, but failed to match the pattern"))))

(defn ->session+channel [url]
  (let [user        (or (config :sftp.user)
                        (env "SFTP_USER")
                        (env "SSH_USER")                    ; backward compatibility
                        (die "Either (uio/with {:sftp.user} ... ) or env SFTP_USER was expected to be set"))
        
        known-hosts (or (config :sftp.known-hosts)
                        (env "SFTP_KNOWN_HOSTS")
                        (env "SSH_KNOWN_HOSTS")             ; backward compatibility
                        (die "Either (uio/with {:sftp.known-hosts} ... ) or env SFTP_KNOWN_HOSTS was expected to be set"))

        pass        (or (config :sftp.pass)
                        (env "SFTP_PASS")
                        (env "SSH_PASS"))                   ; backward compatibility

        id          (or (config :sftp.identity)
                        (env "SFTP_IDENTITY")
                        (env "SSH_PRIVATE_KEY"))            ; backward compatibility

        id-pass     (or (config :sftp.identity.pass)
                        (config :sftp.identity.passphrase)  ; backward compatibility
                        (env "SFTP_IDENTITY_PASS")
                        (env "SSH_PASSPHRASE"))             ; backward compatibility

        _           (if-not (or pass id)
                      (die "Expected :sftp.pass (env SFTP_PASS) or :sftp.identity (SFTP_IDENTITY) to be present, but got neither"))

        j           (JSch.)
        _           (.setKnownHosts j (ByteArrayInputStream. (.getBytes known-hosts))) ; seems to be ignored if private key is not encrypted
        _           (if id
                      (.addIdentity j "uio-identity"
                                    (.getBytes (reformat-private-key-if-needed id))
                                    nil
                                    (.getBytes (or id-pass ""))))

        s           (.getSession j user (host url) (or (port url) 22))
        _           (some->>  (.setPassword s pass))
        _           (.connect s)

        c           (doto (.openChannel s "sftp")
                          (.connect))]
    [s c]))

(defn with-channel [url c->x]
  (try-with #(->session+channel url)
            (fn [[_ c]] (c->x c))
            (fn [[s c]] (.disconnect c)
                        (.disconnect s))))

(defmethod from    :sftp [url]             (wrap-is #(->session+channel url)
                                                    (fn [[s c]] (.get c (path url)))
                                                    (fn [[s c]] (.disconnect c)
                                                                (.disconnect s))))

; TODO create all parent dirs?
(defmethod to      :sftp [url]             (wrap-os #(file/->temp-file "uio-sftp-" "-temp.gz")
                                                    #(GZIPOutputStream. (to %))
                                                    #(try
                                                       ; workaround to Jsch concurrency bug
                                                       ; store in a local gzipped file before sending over SFTP
                                                       (with-channel url (fn [c]
                                                                           (.put c
                                                                                 (GZIPInputStream. (from %))
                                                                                 (path url))))
                                                       (finally (delete %)))))

(defmethod size    :sftp [url]             (with-channel url #(.getSize (.stat % (path url)))))

(defmethod exists? :sftp [url]             (try (size url)
                                                true
                                                (catch SftpException e
                                                  (if (= ChannelSftp/SSH_FX_NO_SUCH_FILE (.id e))
                                                    false
                                                    (die "Couldn't determine file existence" {:url url} e)))))

(defmethod delete  :sftp [url]             (with-channel url #(try (if (.isDir (.stat % (path url)))
                                                                     (.rmdir % (path url))
                                                                     (.rm % (path url)))
                                                                   (catch Exception e
                                                                     (die (str "Could not delete " (pr-str url) " -- " (.getMessage e)) {} e)))))

(defmethod mkdir   :sftp [url]             (with-channel url #(try (.mkdir % (path url))
                                                                   (catch Exception e
                                                                     (die (str "Could not create directory at " (pr-str url)  " -- " (.getMessage e)) {} e)))))

(defmethod copy    :sftp [from-url to-url] (try-with #(->session+channel to-url)
                                                     (fn [[_ c]] (with-open [is (from from-url)]
                                                                   (.put c is (path to-url))))
                                                     (fn [[s c]] (.disconnect c)
                                                                 (.disconnect s))))

(defmethod ls      :sftp [url & [opts]]
  ; TODO explicitly release resources after migrating to reusable/scoped sessions
  (let [[s c] (->session+channel url)
        z (->Finalizer s c)]                                ; a trick to release resources upon a) reaching end of the collection or b) being collected by GC
    (lazy-cat
      ; create a recursive fn and call it
      ((fn -ls [url z]                                      ; prevent finalizer from being GCed while this fn is still referenced by someone
         (->> (try (.ls c (path url))
                   (catch Exception e (die "Couldn't list files for" {:url url} e)))
              (sort-by #(.getFilename %))

              ; TODO implement symlink behavior

              (mapcat #(let [filename (.getFilename %)
                             file-url (with-parent url (encode-url filename))]
                         (cond ; skip "." and ".." dirs
                             (and (.isDir (.getAttrs %))
                                  (#{"." ".."} filename))
                             nil

                             ; dir => enlist + recurse if asked
                             (.isDir (.getAttrs %))
                             (cons {:url file-url
                                    :dir true}
                                   (if (:recurse opts)
                                     (lazy-seq (-ls file-url z))
                                     nil))

                             ; file => enlist
                             :else
                             [{:url  file-url
                               :size (.getSize (.getAttrs %))}])))))

        ; fn args -- starting dir
        (str (.replaceAll url "/+$" "") default-delimiter)
        z)

      ; close session/channel upon reaching the end of the sequence (without waiting for GC).
      ; NOTE: (lazy-cat xs ys zs) === (concat (lazy-seq xs) (lazy-seq ys) (lazy-seq zs))
      ;       meaning, that it's `(lazy-seq (.finalize z))` and it will be called only when the end is reached.
      (.finalize z))))
