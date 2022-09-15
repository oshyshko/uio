; SFTP
;
; sftp://host[:port]/path/to/file.txt
;
;  :user
;
;  :pass
;     -- OR --
;  :identity                            <-- actual content
;  :identity-pass           (optional)  <-- (if needed)
;
;  :known-hosts             (optional)  <-- actual content (ssh-rsa), if not specified
;  :skip-owner-group-lookup (optional)  <-- defaults to false, disables mapping of user/group ids to names,
;                                           can cause `ls` and `attrs` to hang if shell execution is disabled
;
;   NOTE: To get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa [-p <port>] <host>`
;         and copy the content (skip the line starting with a #).
;
(ns uio.fs.sftp
  (:require [clojure.string :as str]
            [uio.fs.file :as file]
            [uio.impl :refer :all])
  (:import [com.jcraft.jsch JSch ChannelSftp Session SftpException SftpATTRS Channel]
           [java.io ByteArrayInputStream]
           [java.util.zip GZIPOutputStream GZIPInputStream]
           [java.util Date]
           (clojure.lang IPersistentMap)))

(def default-timeout-ms 10000)

(def ^:dynamic *sft-connection-config* {:connection-timeout default-timeout-ms})

(defn with-sftp-configs [config f]
  (if-not (instance? IPersistentMap config)
    (die (str "Argument `config` expected to be a map, but was " (.getName (class config)))))

  (binding [*sft-connection-config* (merge *sft-connection-config* config)]
    (f)))

; JSch expects a private key with new-line characters as described in RFC-4716.
; However, it's useful  to pass private keys around as a single-line string where new-lines are replaced with space.
; This fn will convert a single-line private key back to multi-line format and make JSch happy.
(defn reformat-private-key-if-needed [s]
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

(defn ->session+channel [url]                               ; -> [Session ChannelSftp]
  (let [{:keys [user
                known-hosts
                skip-owner-group-lookup
                pass
                identity
                identity-pass]} (url->creds url)

        _ (if-not user                           (die "Expected :user, but got none"))
        _ (if-not (or pass identity)             (die "Expected either :pass or :identity to be present, but got neither"))
        _ (if (and identity-pass (not identity)) (die "Got :identity-pass without :identity"))

        j (JSch.)
        _ (when known-hosts
            (.setKnownHosts j (ByteArrayInputStream. (.getBytes known-hosts)))) ; seems to be ignored if private key is not encrypted

        _ (if identity
            (.addIdentity j
                          "uio-identity"
                          (.getBytes (reformat-private-key-if-needed
                                       (if (str/starts-with? identity "file://")
                                         (slurp identity)
                                         identity)))
                          nil                               ; pubkey
                          (.getBytes (or identity-pass ""))))

        s (.getSession j user (host url) (or (port url) 22)) ; ^Session
        _ (.setTimeout s (:connection-timeout *sft-connection-config*))
        _ (println (str "Connection timeout is " (:connection-timeout *sft-connection-config*)))
        _ (.setConfig s "StrictHostKeyChecking" (if known-hosts "yes" "now"))
        _ (.setPassword s pass)
        _ (.connect s)

        c (doto (.openChannel s "sftp")                     ; ^Channel
                (.connect default-timeout-ms))]
    [s c]))

(defn with-session-channel [url session-channel->x]
  (try-with url
            #(->session+channel url)
            (fn [[s c]] (session-channel->x s c))
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
                                                  (with-session-channel url
                                                                        (fn [_ c]
                                                                          (.put c
                                                                                (GZIPInputStream. (from %))
                                                                                (path url))))
                                                  (finally (delete %)))))

(defmethod exists? :sftp [url & args] (with-session-channel url
                                                            (fn [_ c]
                                                              (try (.getSize (.stat c (path url)))
                                                                   true
                                                                   (catch SftpException e
                                                                     (if (= ChannelSftp/SSH_FX_NO_SUCH_FILE (.id e))
                                                                       false
                                                                       (die (str "Couldn't determine file existence " url) e)))))))

(defmethod delete  :sftp [url & args] (with-session-channel url
                                                            (fn [_ c]
                                                              (rethrowing (str "Could not delete " url)
                                                                          (if (.isDir (.stat c (path url)))
                                                                            (.rmdir c (path url))
                                                                            (.rm c (path url)))))))

(defmethod mkdir   :sftp [url & args]      (with-session-channel url
                                                                 (fn [_ c]
                                                                   (rethrowing
                                                                     (str "Could not create directory at " url)
                                                                     (try
                                                                       (.mkdir c (path url))
                                                                       (catch Exception e
                                                                         (if (exists? url)
                                                                           nil ; it's already there
                                                                           (if (or (not (parent-of url)) ; root dir or has parent = die
                                                                                   (exists? (parent-of url)))
                                                                             (throw e)
                                                                             ; TODO refactor to avoid 2 extra calls of (exists? ...)
                                                                             (mkdirs-up-to url #(.mkdir c (path %)))))))))))

(defmethod copy    :sftp [from-url to-url & args] (try-with to-url
                                                            #(->session+channel to-url)
                                                            (fn [[_ c]]
                                                              (with-open [is (from from-url args)]
                                                                (.put c is (path to-url))))
                                                            (fn [[s c]]
                                                              (.disconnect c)
                                                              (.disconnect s))))

(defn f->attrs [^Channel c uid->name gid->name file-url extended? ^SftpATTRS a]
  (merge {:url (str file-url (if (.isDir a) default-delimiter))}
         (if (.isDir a)
           {:dir true}
           {:size (.getSize a)})

         (if extended?
           (merge {:accessed (-> a .getATime (* 1000) Date.)
                   :modified (-> a .getMTime (* 1000) Date.)
                   :owner    (or (uid->name (.getUId a)) (.getUId a))
                   :group    (or (gid->name (.getGId a)) (.getGId a))
                   :perms    (-> a .getPermissionsString (subs 1))}

                  (if (.isLink a)
                    {:symlink (->> (.readlink c (path file-url))
                                   (str (parent-of file-url))
                                   normalize)})))))

(defn -ls [c uid->name gid->name url recurse? extended?]
  (try (->> (concat (.ls c (str (path url) "/*"))
                    (.ls c (str (path url) "/.*")))
            (sort-by #(.getFilename %))
            (mapcat #(let [f        (.getFilename %)        ; % :: ChannelSftp$LsEntry
                           file-url (with-parent url f)]
                       (if-not (#{"." ".."} f)              ; skip "." and ".." dirs
                           (cons (f->attrs c uid->name gid->name file-url extended? (.getAttrs %))
                                 (if (and recurse?
                                          (.isDir (.getAttrs %))) ; if isDir=true then isLink=false
                                   (lazy-seq (-ls c uid->name gid->name file-url recurse? extended?))
                                 nil))
                         nil))))
    (catch Exception e [{:url url :error e}])))

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
                (.connect default-timeout-ms))]             ; if not set, this will hang forever for hosts that don't allow shell
    (try
      (slurp (.getInputStream c))
      (finally (.disconnect c)))))

(defn ->uid->name [^Session s]
  (try (passwd->id->name (exec->s s "getent passwd"))
       (catch Exception _ {})))

(defn ->gid->name [^Session s]
  (try (passwd->id->name (exec->s s "getent group"))
       (catch Exception _ {})))

(defmethod attrs :sftp [url & args] (with-session-channel url
                                                          (fn [s c]
                                                            (let [creds (url->creds url)]
                                                              (f->attrs c
                                                                        (if (:skip-owner-group-lookup creds) {} (->uid->name s))
                                                                        (if (:skip-owner-group-lookup creds) {} (->gid->name s))
                                                                        url
                                                                        true
                                                                        (.stat c (path url)))))))

(defmethod ls :sftp [url & args] (single-file-or
                                   url
                                   (let [opts     (get-opts default-opts-ls url args)
                                         creds    (url->creds url)
                                         [s c]    (->session+channel url)
                                         close-cs #(do (.disconnect c)
                                                       (.disconnect s))]
                                     (try
                                       (close-when-realized-or-finalized
                                         close-cs
                                         (-ls c
                                              (or (and (:attrs opts)
                                                       (not (:skip-owner-group-lookup creds))
                                                       (->uid->name s))
                                                  {})

                                              (or (and (:attrs opts)
                                                       (not (:skip-owner-group-lookup creds))
                                                       (->gid->name s))
                                                  {})

                                              (ensure-not-ends-with-delimiter (normalize url))
                                              (:recurse opts)
                                              (:attrs opts)))
                                       (catch Exception _ (close-cs))))))
