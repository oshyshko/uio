(ns uio.impl
  (:require [clojure.java.io :as jio]
            [clojure.string :as str])
  (:import [clojure.lang IFn IPersistentMap Keyword]
           [java.io ByteArrayInputStream ByteArrayOutputStream Closeable FilterInputStream FilterOutputStream InputStream OutputStream]
           [java.net URI URLDecoder URLEncoder]
           [java.security Security]
           [uio.fs Streams$CountableInputStream Streams$CountableOutputStream Streams$DigestibleInputStream Streams$DigestibleOutputStream Streams$NullOutputStream Streams$Finalizer]))

(def default-delimiter "/")
(def default-opts-from {:offset 0
                        :length nil})
(def default-opts-ls   {:recurse false
                        :attrs   false})

; Helper fns

; Examples:
; (die "MUHAHA!")
; (die "Can't find keys" {:what "keys" :to "my car"})
; (die "This url is misbehaving" {:url url} ioe)
(defn die "A shortcut for (throw (ex-info ...))"
  ([^String msg]       (throw (Exception. msg)))
  ([^String msg cause] (throw (Exception. msg cause))))

; Example:
; (let [file "/non-existent/path/to/file.txt"]
;   (rethrowing "Couldn't write to a file" {:file file}
;     (spit file "ignore")))
;
(defmacro rethrowing
  "Evaluate body, and if any exception is thrown then wrap that
   exception into an ex-info with the specified msg and map before rethrowing it."
  [msg & body]
  ; TODO assert msg is a String
  `(try (do ~@body)
        (catch Exception e# (die (str ~msg ". " (.getMessage e#)) e#))))



; URL manipulation (not really a part of public API)
;
; "foo://user@host:8080/some-dir/file.txt?k=v&x=y&x=z"           <- URL
; "foo"                                                          <- (scheme ...)
;       "user"                                                   <- (user ...)
;            "host"                                              <- (host ...)
;                  8080                                          <- (port ...)   ; (int or nil)
;                     "/some-dir/file.txt"                       <- (path ...)
;                               "file.txt"                       <- (filename ...)
;                                        "k=v&x=y&x=z"           <- (query ...)
;                                       {:k ["v"], :x ["y" "z"]} <- (query-map ...)
;

(def pattern-url-single-delimiter #"^([a-zA-Z]?[a-zA-Z0-9+-]+):/([^/].*)?")
(def pattern-url-no-auth-and-path #"^([a-zA-Z]?[a-zA-Z0-9+-]+)://(\?.*)?")

(defn fix-url [url]
  (cond (re-matches pattern-url-single-delimiter url)
        (str/replace-first url ":/" ":///")

        (re-matches pattern-url-no-auth-and-path url)
        (str/replace-first url "://" ":///")

        :else url))

(defn ->URI          ^URI    [^String url] (rethrowing
                                             (str "Couldn't parse URL " (pr-str url))
                                             (let [fixed-url      (fix-url url)
                                                   normalized-uri (.normalize (URI. fixed-url))
                                                   normalized-uri-str (str normalized-uri)]
                                               (if (or (= "/" (.getSchemeSpecificPart normalized-uri))
                                                       (re-matches pattern-url-single-delimiter normalized-uri-str))
                                                 (URI. (str/replace-first normalized-uri-str ":/" ":///"))
                                                 normalized-uri))))

(defn url?          ^Boolean [^String url] (try (->URI url)
                                                true
                                                (catch Exception _ false)))

(defn normalize     ^String  [^String url] (-> url ->URI str))
(defn scheme        ^String  [^String url] (-> url ->URI .getScheme))
(defn scheme-k      ^Keyword [^String url] (-> url ->URI .getScheme keyword))

(defn user          ^String  [^String url] (-> url ->URI .getUserInfo))
(defn host          ^String  [^String url] (-> url ->URI .getHost))
(defn port          ^Integer [^String url] (let [p (-> url ->URI .getPort)]
                                             (if (not= -1 p) p)))
(defn path          ^String  [^String url] (let [p (.getPath (->URI url))]
                                             (if (and (not (str/blank? p))
                                                      (not (re-matches pattern-url-no-auth-and-path url)))
                                               p)))
(defn filename      ^String  [^String url] (let [s (-> url path)]
                                             (subs s (inc (str/last-index-of s default-delimiter)))))

(defn escape-url    ^String  [^String s]   (.replace (URLEncoder/encode s "UTF-8") "+" "%20"))
(defn unescape-url  ^String  [^String s]   (URLDecoder/decode s "UTF-8")) ; TODO replace %20/+?

(defn query         ^String  [^String url] (.getRawQuery (->URI url)))
(defn query-map              [^String url] (if-let [q (query url)]
                                             (->> q
                                                  (#(str/split % #"&")) ; split key-value pairs
                                                  (map #(str/split % #"=")) ; split key from from values
                                                  (map #(map unescape-url %))
                                                  (map (fn [[k v]] [(if-not (str/blank? k) (keyword k))
                                                                    (if (some? v) [v] [])]))
                                                  (reduce (fn [s [k v]]
                                                            (merge-with into s {k v}))
                                                          {}))
                                             {}))
(defn but-query     ^String [^String url] (if-let [q (query url)]
                                            (subs url 0 (dec (str/index-of url q)))
                                            url))
; Public API
(defmulti from    (fn [^String url & args] (scheme-k url)))    ; -> InputStream  -- must be closed by user, use (with-open [...])
(defmulti to      (fn [^String url & args] (scheme-k url)))    ; -> OutputStream -- must be closed by user, use (with-open [...])
(defmulti size    (fn [^String url & args] (scheme-k url)))    ; -> Number
(defmulti exists? (fn [^String url & args] (scheme-k url)))    ; -> boolean
(defmulti delete  (fn [^String url & args] (scheme-k url)))    ; -> nil
(defmulti ls      (fn [^String url & args] (scheme-k url)))    ; -> []
(defmulti mkdir   (fn [^String url & args] (scheme-k url)))    ; -> nil
(defmulti attrs   (fn [^String url & args] (scheme-k url)))    ; -> nil
(defmulti copy    (fn [^String from-url ^String to-url & args] ; -> nil
                    (->URI from-url)                           ; ensure `from-url` is also parsable
                    (scheme-k to-url)))                        ; dispatch on `scheme` (and ensure it's also parsable)

; Codecs
(defmulti ext->is->is (fn [^String ext] ext)) ; ext -> (fn [^InputStream  is] ...wrap into another InputStream)
(defmulti ext->os->os (fn [^String ext] ext)) ; ext -> (fn [^OutputStream os] ...wrap into another OutputStream)

; Other helper fns (for implementations)

(def ^:dynamic *config* {})

(defn longest-matching-prefix [cred-prefix-urls s]
  (->> cred-prefix-urls
       (filter #(str/starts-with? s %))
       (sort-by count)
       last))

; TODO parse env and build config
; UIO_URL_A=s3://bucket-a?access=...-a&secret=...
; UIO_URL_B=sftp://host:port?user=...&pass=...&custom-param=...
;
(defn creds-url->creds [^String url]
  {(but-query url)
   (->> (query-map url)
        (map (fn [[k [v :as vs]]]
               (if-not (some? k)        (die (str "Got an empty key in credentials URL starting with: " (but-query url))))
               (if-not (= 1 (count vs)) (die (str "Got multiple " (pr-str (name k)) " keys in credentials URL starting with: " (but-query url))))
               [k v]))
        (into {}))})

; for testing, see `url->creds` for API
(defn url->creds' [config env url]
  (let [longest-url (longest-matching-prefix (filter #(and (string? %)
                                                           (scheme %))
                                                     (keys config))
                                             url)
        cr          (or (get config longest-url) {})        ; credentials (value) extracted by URL (key)
        c           (or config {})                          ; config -- for compatibility, credentials stored as keys
        e           (or env {})                             ; env    -- for compatibility, comes from JVM process (immutable, extracted as arg for testing)

        nie         (fn [s] (if (str/blank? s) nil s))      ; nil-if-empty
        die-no-key  (fn [k]
                      (if longest-url
                        (die (str "Could not locate " k " key in among keys " (keys cr) " for credentials URL " longest-url))
                        (die (str "Could not locate credentials for URL: " url))))

        eu          (fn [k url-or-path]                     ; ensure-url
                      (cond (nil? url-or-path) nil
                            (str/starts-with? url-or-path default-delimiter) (str "file://" url-or-path)
                            (url? url-or-path) url-or-path
                            :else (die (str "Expected URL or path that starts with / for " k ", but got: " url-or-path))))
        ]

    ; TODO post-validate pairs?
    ; TODO fail on unknown keys in `cr`?
    (case (scheme url)
      ;       <current>                        <current>          <obsolete>                    <obsolete>                  <obsolete>
      "hdfs" {:principal          (nie (or (cr :principal)     (c :hdfs.keytab.principal)    (e "HDFS_KEYTAB_PRINCIPAL") (e "KEYTAB_PRINCIPAL")))
              :keytab (eu :keytab (nie (or (cr :keytab)        (c :hdfs.keytab.path)         (e "HDFS_KEYTAB_PATH")      (e "KEYTAB_FILE"))))
              :access                  (or (cr :access)        (c :s3.access)                (e "AWS_ACCESS")            (e "AWS_ACCESS_KEY_ID"))
              :secret                  (or (cr :secret)        (c :s3.secret)                (e "AWS_SECRET")            (e "AWS_SECRET_ACCESS_KEY"))}

      "sftp" {:user                    (or (cr :user)          (c :sftp.user)                (e "SFTP_USER")             (e "SSH_USER")               (die-no-key :user))
              :known-hosts             (or (cr :known-hosts)   (c :sftp.known-hosts)         (e "SFTP_KNOWN_HOSTS")      (e "SSH_KNOWN_HOSTS")        (die-no-key :known-hosts))
              :pass                    (or (cr :pass)          (c :sftp.pass)                (e "SFTP_PASS")             (e "SSH_PASS"))
              :identity                (or (cr :identity)      (c :sftp.identity)            (e "SFTP_IDENTITY")         (e "SSH_PRIVATE_KEY"))
              :identity-pass           (or (cr :identity-pass) (c :sftp.identity.pass)
                                                               (c :sftp.identity.passphrase) (e "SFTP_IDENTITY_PASS")    (e "SSH_PASSPHRASE"))})))

(defn url->creds [url]
  (url->creds' *config* (into {} (System/getenv)) url))

(defn wrap-is [->resource resource->is close-resource]
  (let [r       (->resource)
        *closed (atom false)]
    (proxy [FilterInputStream] [(resource->is r)]
      (close [] (when-not @*closed                          ; TODO remove race condition chance
                  (try (proxy-super close)
                       (finally (close-resource r)))
                  (reset! *closed true))))))                ; TODO remove race condition chance

(defn wrap-os [->resource resource->os close-resource]
  (let [r       (->resource)
        os      (resource->os r)
        *closed (atom false)]
    (proxy [FilterOutputStream] [os]
      ; delegate batch methods as is -- don't peel into individual (.write ... ^int) calls
      ; NOTE: this lowers chances of SFTP implementation to hang (concurrency bug in JSch)
      ;       and improves performance
      (write ([^bytes bs]               (.write os bs))
             ([^bytes bs offset length] (.write os bs offset length)))

      (close [] (when-not @*closed                          ; TODO remove race condition chance
                  (try (proxy-super close)
                       (finally (close-resource r)))
                  (reset! *closed true))))))                ; TODO remove race condition chance

; Example:
; (try-with #(FileInputStream. "file://1.txt")
;           #(.available %)
;           #(.close %))
; => ...returns result of `(.avaiable ...)`
;
(defn try-with [->resource resouce->value close-resource]
  (let [r (->resource)]
    (try (resouce->value r)
         (finally (close-resource r)))))

(defn get-opts [default-opts url args]                      ; {opts} -> [opt-keys] -> url -> [{opts}] -> {opts}
  (if (< 1 (count args))
    (die (str "Expected 0 or 1 arguments for `opts`, but got " (count args) ", for args: " (pr-str args) " and URL: " url)))

  (let [opts            (first args)                        ; TODO assert opts is a map or nil + there's no second arg
        unexpected-args (reduce dissoc opts (keys default-opts))]
    (when-not (empty? unexpected-args)
      (die (str "Got unsupported options: " (pr-str unexpected-args)
                ". Supported options are: " (pr-str (keys default-opts)))))

    (merge default-opts opts)))

; helper fns to support directories and recursive operations

(defn ends-with-delimiter? [url]
  (str/ends-with? (or (path url) "") default-delimiter))

(defn ensure-ends-with-delimiter [url]
  (if (ends-with-delimiter? url)
    url
    (str url default-delimiter)))

(defn ensure-not-ends-with-delimiter [^String url]
  (if (str/ends-with? url default-delimiter)
    (recur (.substring url 0 (- (count url) 1)))
    url))

; Example:
; (get-parent-dir "/" "1/2/3.txt")
; => "1/2/"
(defn parent-of                                                ; -> [String] (does not end with a slash)
  ([^String url] (parent-of default-delimiter url))
  ([^String delimiter ^String url]
   (when-let [i (str/last-index-of url delimiter)]
     (subs url 0 (inc i)))))

(defn with-parent [^String parent-url ^String file]
  (if (str/includes? file default-delimiter)
    (die (str "Expected argument \"file\" to have no directory delimiters, but it had: " (pr-str file))))
  (str (ensure-ends-with-delimiter parent-url) file))

(defn replace-path [^String url ^String absolute-path]
  (if-not (str/starts-with? absolute-path default-delimiter)
    (die (str "Expected argument \"absolute-path\" to start with a directory delimiter, but was: " (pr-str absolute-path))))
  (str (subs url
             0
             (- (count url)
                (count (path url))))
       absolute-path))

; See "test_uio.clj", (facts "intercalate-with-dirs works" ...)
(defn intercalate-with-dirs
  ([kvs]           (intercalate-with-dirs default-delimiter kvs))
  ([delimiter kvs] (intercalate-with-dirs nil delimiter kvs))
  ([delimiter last-flushed-dir [kv & kvs]]
   (if-not kv
     []
     (let [parent-dir (parent-of (:url kv))
           flush-dir? (and parent-dir
                           (neg? (compare last-flushed-dir parent-dir)))

           tail       (lazy-seq
                        (intercalate-with-dirs delimiter
                                               (if flush-dir?
                                                 parent-dir
                                                 last-flushed-dir)
                                               kvs))]
       (cond->> (cons kv tail)
                flush-dir? (cons {:url parent-dir
                                  :dir true}))))))

; codec-related fns
;
(defn ensure-has-all-impls [url ext+s->s]
  (if (not-every? (comp some? second) ext+s->s)
    (die (str "Got at least one unsupported codec: " (mapv first (remove second ext+s->s))
              ". Available extensions are: " (mapv first ext+s->s)
              ". The URL that caused trouble was: " url))
    ext+s->s))

; Based on file extensions of given url, build a sequence of pairs: extension + stream->stream codec function.
; Where `ext->s->s` is either `ext->is->is` or `ext->os->os` (depending on whether you're reading or writing)"
;
; Example (reading):
; (url->ext+s->s ext->is->is "file:///path/to/file.txt.xz.bz2.gz")
; => [[:gz  (fn [^InputStream] ...another InputStream)]]
;     [:bz2 (fn [^InputStream] ...another InputStream)]]
;     [:xz  (fn [^InputStream] ...another InputStream)]]]
;
; Example (writing):
; (url->ext+s->s ext->os->os "file:///path/to/file.txt.xz.bz2.gz")
; => [[:gz  (fn [^OutputStream] ...another OutputStream)]]
;     [:bz2 (fn [^OutputStream] ...another OutputStream)]]
;     [:xz  (fn [^OutputStream] ...another OutputStream)]]]
;
; Example (of an unknown codec in the middle):
; (url->ext+s->s ext->os->os "file:///path/to/file.txt.xz.ufoz.bz2.gz")
; =!> clojure.lang.ExceptionInfo: Got at least one unsupported codec.
;     Consider adding adding `:codecs false` option or implementing a codec for the unknown extension
;     {:unsupported [:ufoz],
;      :exts        [:gz :bz2 :ufoz :xz],
;      :url         "file:///path/to/file.txt.xz.ufoz.bz2.gz"}, compiling:
;
(defn url->ext+s->s
  [ext->s->s ^String url]
  (->> (str/split (path url) #"\.")
       (map keyword)
       (map #(vector % (ext->s->s %)))
       (drop-while (comp nil? second))
       reverse
       (ensure-has-all-impls url)))

; Example (reading):
; (apply-codecs (from "file:///path/to/file.txt.xz.gz")
;               (url->ext+s->s ext->is->is "file:///path/to/file.txt.xz.gz"))
; => InputStream ...that reads from file:///path/to/file.txt.xz.gz, decompresses with GZIP, then XZ
;
; Example (writing):
; (apply-codecs (to "file:///path/to/file.txt.xz.gz")
;               (url->ext+s->s ext->os->os "file:///path/to/file.txt.xz.gz"))
; => OutputStream ...that compresses with XZ, then GZIP and writes to file:///path/to/file.txt.xz.gz
;
(defn apply-codecs
  "Wrap given `InputStream` or `OutputStream` with codecs specified in `ext+s->s` sequence"
  [s ext+s->s]
  (reduce (fn [s [ext s->s]]
            (rethrowing (str "Problem with " ext)
                        (s->s s)))
          s
          ext+s->s))

; streams<->bytes functions
(defn ^bytes is->bytes [^InputStream is]
  (let [baos (ByteArrayOutputStream.)]
    (jio/copy is baos)
    (.toByteArray baos)))

(defn ^InputStream bytes->is [^bytes bs]
  (ByteArrayInputStream. bs))

(defn ^bytes with-baos->bytes [^IFn baos->nil]
  (let [baos (ByteArrayOutputStream.)]
    (baos->nil baos)
    (.toByteArray baos)))

; encoding/decoding: streams + bytes
(defn decode [^Keyword ext is-or-bytes] ;
  (cond (instance? InputStream          is-or-bytes) ((ext->is->is ext) is-or-bytes)
        (instance? (Class/forName "[B") is-or-bytes) (with-baos->bytes
                                                       #(with-open [is (decode ext (bytes->is is-or-bytes))
                                                                    os %]
                                                          (jio/copy is os)))
        :else                                        (die (str "Expected InputStream or OutputStream, but got "
                                                               (if (nil? is-or-bytes)
                                                                 "nil"
                                                                 (.getName (type is-or-bytes)))))))
(defn encode [^Keyword ext os-or-bytes]
  (cond (instance? OutputStream         os-or-bytes) ((ext->os->os ext) os-or-bytes)
        (instance? (Class/forName "[B") os-or-bytes) (with-baos->bytes
                                                       #(with-open [is (bytes->is os-or-bytes)
                                                                    os (encode ext %)]
                                                          (jio/copy is os)))
        :else                                        (die (str "Expected InputStream or OutputStream, but got "
                                                               (if (nil? os-or-bytes)
                                                                 "nil"
                                                                 (.getName (type os-or-bytes)))))))
; other streams functions
(defn ^OutputStream ->nil-os []
  (Streams$NullOutputStream.))


; Count how many bytes came through a stream.
;
; Example:
; (with-open [is (counted-is (bytes->is (.getBytes "hello world")))]
;   (println (count is))
;   (.read is)
;   (println (count is)))
; 0
; 1
; => nil
; (with-open [os (counted-os nil)]
;   (println (count os))
;   (.write os (.getBytes "hello world"))
;   (println (count os)))
; 0
; 11
; => nil
;
; Example (advanced): measure compressed VS uncompressed ratio
; (with-open [os-file (->countable (jio/output-stream (->nil-os)))
;             os-gz   (->countable (ext-encode-os :gz os-file))
;             w       (jio/writer os-gz)]
;
;   ; ==> (.write {Writer                           --> w
;   ;               {CountableOutputStream          --> os-gz
;   ;                 {GZIPOutputStream
;   ;                   {CountableOutputStream      --> os-file
;   ;                     {NullOutputStream}}}}}
;
;   (doseq [i (range 10000)]
;     (.write w (str i "\n")))
;
;   ; Close the outer os/writer to flush all underlying os/writers.
;   ; In this way we can get final counters, while still in `with-open` block.
;   (.close w)
;
;   {:original   (count os-gz)
;    :compressed (count os-file)})
;
;  => {:original 48890, :compressed 22196}
;
(defn ->countable [^Closeable is-or-os]
  (cond (instance? InputStream  is-or-os) (Streams$CountableInputStream.  is-or-os)
        (instance? OutputStream is-or-os) (Streams$CountableOutputStream. is-or-os)
        :else                             (die (str "Expected InputStream or OutputStream, but got "
                                                    (if (nil? is-or-os)
                                                      "nil"
                                                      (.getName (type is-or-os)))))))

; Adding digest calculation to existing input and output streams.
;
; Examples:
; (with-open [is (->digestible "MD5" (bytes->is (.getBytes "hello world")))]
;   (loop []
;     (if (not= -1 (.read is))
;       (recur)))
;   (javax.xml.bind.DatatypeConverter/printHexBinary (close-and-digest is)))
;  => "5EB63BBBE01EEED093CB22BB8F5ACDC3"
;
; (with-open [os (->digestible "MD5" (->nil-os))]
;   (.write os (.getBytes "hello world"))
;   (javax.xml.bind.DatatypeConverter/printHexBinary (close-and-digest os)))
; => "5EB63BBBE01EEED093CB22BB8F5ACDC3"
;
; $ echo -n "hello world" | md5
; 5eb63bbbe01eeed093cb22bb8f5acdc3
;
(defn ->digestible [^String algorithm ^Closeable is-or-os]
  (cond (instance? InputStream  is-or-os) (Streams$DigestibleInputStream.  algorithm is-or-os)
        (instance? OutputStream is-or-os) (Streams$DigestibleOutputStream. algorithm is-or-os)
        :else                             (die (str "Expected InputStream or OutputStream, but got "
                                                    (if (nil? is-or-os)
                                                      "nil"
                                                      (.getName (type is-or-os)))))))

(defn close-and-digest [^Closeable is-or-os] (.closeAndDigest is-or-os))

; ["MD2" "MD5" "SHA" "SHA-224" "SHA-256" "SHA-384" "SHA-512"] <-- for JDK 1.8.0_121-b13
(def available-digest-algorithms (for [p (Security/getProviders)
                                       s (.getServices p)
                                       :when (= "MessageDigest" (.getType s))]
                                   (.getAlgorithm s)))

; Rest of Public API
(defn with-fn [config f]
  (if-not (instance? IPersistentMap config)
    (die (str "Argument `config` expected to be a map, but was " (.getName (class config)))))

  (binding [*config* (merge *config* config)]
    (f)))

; TODO resolve clash with uio/with
;(defmacro with [config & body]
;  `(with-fn ~config (fn [] ~@body)))

; TODO add examples
(defn ^InputStream from* [^String url & args]
  (rethrowing (str "Couldn't apply is->is codecs to " url)
              (apply-codecs (apply from (cons url args))
                            (url->ext+s->s ext->is->is url))))

; TODO add examples
(defn ^OutputStream to* [^String url & args]
  (rethrowing (str "Couldn't apply os->os codecs to" url)
              (apply-codecs (apply to (cons url args))
                            (url->ext+s->s ext->os->os url))))

; Implementation: defaults
(defn default-impl [^String url ^String method args]
  (if (scheme url)
    (die (str "Method " (pr-str method) " is not implemented for " url))
    (die (str "Expected a URL with a scheme, but got: \"" url "\". "
              "Available schemes are: "
              (->> method
                   symbol resolve deref methods keys
                   (remove #{:default})
                   (map #(str (name %) "://"))
                   sort
                   (str/join ", "))))))

(defmethod from    :default [url & args] (default-impl url "from"    args))
(defmethod to      :default [url & args] (default-impl url "to"      args))
(defmethod size    :default [url & args] (default-impl url "size"    args))
(defmethod exists? :default [url & args] (default-impl url "exists?" args))
(defmethod delete  :default [url & args] (default-impl url "delete"  args))
(defmethod ls      :default [url & args] (default-impl url "ls"      args))
(defmethod mkdir   :default [url & args] (default-impl url "mkdir"   args))
(defmethod attrs   :default [url & args] (default-impl url "attrs"   args))

(defmethod copy    :default [from-url to-url & args] (with-open [is (from from-url)
                                                                 os (to to-url)]
                                                       (jio/copy is os :buffer-size 8192)))
(defmethod ext->is->is :default [_] nil)
(defmethod ext->os->os :default [_] nil)

; Example:
; (ns myns
;   (:require [uio.uio :as uio]      ; loads implementation
;             [uio.impl :as impl]))
;
; (uio.impl/list-available-implementations)
; => {:fs     [:file :hdfs :http :https :mem :res :s3 :sftp]
;     :codecs [:bz2 :gz :xz]}
;
(defn list-available-implementations []
  {:fs     (-> (.getMethodTable from)        (dissoc :default) keys sort vec)
   :codecs (-> (.getMethodTable ext->is->is) (dissoc :default) keys sort vec)})

(defn close-when-realized-or-finalized [->close xs]
  (let [f (Streams$Finalizer. ->close)]
    (concat xs (lazy-seq (.close f)))))
