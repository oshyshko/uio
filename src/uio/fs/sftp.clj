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
  (:import [com.jcraft.jsch JSch Channel ChannelSftp ChannelSftp$LsEntry Session SftpException]
           [java.io ByteArrayInputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]
           [java.util Date]))

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

        ^Session s  (.getSession j user (host url) (or (port url) 22))
        _           (some->> (.setPassword s pass))
        _           (.connect s)

        ^Channel c  (doto (.openChannel s "sftp")
                          (.connect))]
    [s c]))

(defn with-channel [url c->x]
  (try-with #(->session+channel url)
            (fn [[_ c]] (c->x c))
            (fn [[s c]] (.disconnect c)
                        (.disconnect s))))

; TODO implement offset + length
(defmethod from    :sftp [url & args] (wrap-is #(->session+channel url)
                                               (fn [[s c]] (.get c (path url)))
                                               (fn [[s c]] (.disconnect c)
                                                           (.disconnect s))))
; TODO create all parent dirs?
; TODO include url in exception (all methods)
(defmethod to      :sftp [url & args] (wrap-os #(file/->temp-file "uio-sftp-" "-temp.gz")
                                               #(GZIPOutputStream. (to %))
                                               #(try
                                                  ; workaround to Jsch concurrency bug
                                                  ; store in a local gzipped file before sending over SFTP
                                                  (with-channel url (fn [c]
                                                                      (.put c
                                                                            (GZIPInputStream. (from %))
                                                                            (path url))))
                                                  (finally (delete %)))))

(defmethod size    :sftp [url & args] (with-channel url #(.getSize (.stat % (path url)))))

(defmethod exists? :sftp [url & args] (try (size url)
                                           true
                                           (catch SftpException e
                                             (if (= ChannelSftp/SSH_FX_NO_SUCH_FILE (.id e))
                                               false
                                               (die "Couldn't determine file existence" {:url url} e)))))

(defmethod delete  :sftp [url & args] (with-channel url #(try (if (.isDir (.stat % (path url)))
                                                                (.rmdir % (path url))
                                                                (.rm % (path url)))
                                                              (catch Exception e
                                                                (die (str "Could not delete " (pr-str url) " -- " (.getMessage e)) {} e)))))

(defmethod mkdir   :sftp [url & args]      (with-channel url #(try (.mkdir % (path url))
                                                                   (catch Exception e
                                                                     (die (str "Could not create directory at " (pr-str url)  " -- " (.getMessage e)) {} e)))))

(defmethod copy    :sftp [from-url to-url & args] (try-with #(->session+channel to-url)
                                                            (fn [[_ c]] (with-open [is (from from-url)]
                                                                          (.put c is (path to-url))))
                                                            (fn [[s c]] (.disconnect c)
                                                              (.disconnect s))))

(defn f->kv [c uid->name gid->name file-url attrs? ^ChannelSftp$LsEntry f]
  (let [a      (.getAttrs f)
        is-dir (.isDir a)]
    (merge {:url (str file-url (if is-dir default-delimiter))}
           (if is-dir
             {:dir true}
             {:size (.getSize (.getAttrs f))})

           (if attrs?
             (merge {:accessed (-> a .getATime (* 1000) Date.)
                     :modified (-> a .getMTime (* 1000) Date.)
                     :owner    (or (uid->name (.getUId a)) (.getUId a))
                     :group    (or (gid->name (.getGId a)) (.getGId a))
                     :perms    (-> a .getPermissionsString)}

                    (if (.isLink a)
                      {:symlink (->> (.readlink c (path file-url))
                                     (str (parent-of file-url))
                                     normalize)}))))))

(defn -ls [c uid->name gid->name url recurse? attrs?]
  (try (->> (concat (.ls c (str (path url) "/*"))
                    (.ls c (str (path url) "/.*")))
            (sort-by #(.getFilename %))
            (mapcat #(let [f        (.getFilename %)
                           file-url (with-parent url (encode-url f))]
                       (if-not (#{"." ".."} f)              ; skip "." and ".." dirs
                         (cons (f->kv c uid->name gid->name file-url attrs? %)
                               (if (and recurse?
                                        (.isDir (.getAttrs %))) ; if isDir=true then isLink=false
                                 (lazy-seq (-ls c uid->name gid->name file-url recurse? attrs?))
                                 nil))
                         nil))))
    (catch Exception e [{:url url :error (str e)}])))

(defn passwd->id->name [s]
  (->> (str/split-lines s)
       (map #(str/split % #":"))
       (reduce (fn [id->name [name _ id :as line]]
                 (try
                   (assoc id->name (Integer/parseInt id) name)
                   (catch Exception _ id->name)))
               {})))

(defn exec->s [s line]
  (let [c (doto (.openChannel s "exec")
                (.setCommand line)
                (.connect))]
    (try
      (slurp (.getInputStream c))
      (finally (.disconnect c)))))

(defmethod ls :sftp [url & args] (let [opts     (get-opts default-opts-ls url args)
                                       [s c]    (->session+channel url)
                                       close-cs #(do (.disconnect c)
                                                     (.disconnect s))

                                       [uid->name gid->name]
                                       (map #(if (:attrs opts)
                                               (try (passwd->id->name (exec->s s %))
                                                    (catch Exception _ {}))
                                               {})
                                            ["getent passwd"
                                             "getent group"])]
                                   (try
                                     (close-when-realized-or-finalized
                                       close-cs
                                       (-ls c
                                            uid->name
                                            gid->name
                                            (ensure-not-ends-with-delimiter (normalize url))
                                            (:recurse opts)
                                            (:attrs opts)))
                                     (catch Exception _ (close-cs)))))
