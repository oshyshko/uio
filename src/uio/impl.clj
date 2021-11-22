(ns uio.impl
  (:require [clojure.java.io :as jio]
            [clojure.string :as str])
  (:import [clojure.lang IFn IPersistentMap Keyword MultiFn]
           [java.io BufferedInputStream BufferedOutputStream BufferedReader BufferedWriter ByteArrayInputStream ByteArrayOutputStream Closeable FilterInputStream FilterOutputStream InputStream OutputStream Reader Writer]
           [java.net URLDecoder URLEncoder URI]
           [java.security Security]
           [uio.fs Streams$ConcatInputStream
                   Streams$DigestibleInputStream
                   Streams$DigestibleOutputStream
                   Streams$Finalizer
                   Streams$FinalizingInputStream
                   Streams$NullOutputStream
                   Streams$Statsable
                   Streams$StatsableInputStream
                   Streams$StatsableOutputStream]
           [java.nio.file AccessDeniedException
                          DirectoryNotEmptyException
                          FileAlreadyExistsException
                          NoSuchFileException
                          NotDirectoryException]))

(def default-delimiter "/")
(def default-opts-from   {:offset 0 :length nil})
(def default-opts-ls     {:recurse false :attrs false})
(def default-opts-delete {:recurse false})

(def minimal-attrs [:url                                    ; always present
                    :dir                                    ; present for dirs only and is always: `:dir true`
                    :size                                   ; present only things that can be rear with `from` (e.g. files, but not dirs)
                    :error])

; neither of the extended attrs is guaranteed to be present
(def extended-attrs [:perms                                 ; "rw-rw-rw-"
                     :modified                              ; #inst"2018-02-09T22:04:53.000-00:00"
                     :owner                                 ; "john"
                     :group])                               ; "staff"

; Helper fns

; Examples:
; (die "MUHAHA!")
; (die (str "Couldn't connect to: " remote-ip) e)
(defn die                     [msg & [cause]] (throw (Exception. msg cause)))

; TODO use everywhere + document
(defn die-no-such-file        [url & [reason]] (throw (NoSuchFileException.        url nil reason)))
(defn die-parent-not-found    [url]            (throw (NoSuchFileException.        url nil "parent directory not found")))
(defn die-access-denied       [url & [reason]] (throw (AccessDeniedException.      url nil reason)))
(defn die-file-already-exists [url & [reason]] (throw (FileAlreadyExistsException. url nil reason)))
(defn die-dir-already-exists  [url]            (throw (FileAlreadyExistsException. url nil "directory already exists")))
(defn die-dir-not-empty       [url]            (throw (DirectoryNotEmptyException. url)))
(defn die-not-a-dir           [url]            (throw (NotDirectoryException.      url)))
(defn die-not-a-file          [url]            (die   (str "Not a file: " (pr-str url))))
(defn die-unsupported         [msg & [cause]]  (throw (UnsupportedOperationException. msg cause)))

(defn die-creds-key-not-found [k url creds]   (if creds
                                                (die (str "Could not locate " k " key among keys " (keys creds) " for credentials URL " url))
                                                (die (str "Could not locate creds for URL: " url))))

(defn die-should-never-reach-here [reason]    (throw (RuntimeException. (str "Should never reach here"
                                                                             (when reason
                                                                               (str ": " reason))))))

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
  (let [url (str/replace url #"\+" "%20")]
    (cond (re-matches pattern-url-single-delimiter url)
          (str/replace-first url ":/" ":///")

          (re-matches pattern-url-no-auth-and-path url)
          (str/replace-first url "://" ":///")

          :else url)))

(defn escape-url ^String [^String s]
  (URLEncoder/encode s "UTF-8"))

(defn escape-path ^String [^String s]
  (-> (URLEncoder/encode s "UTF-8")                         ; keep "+" as "+" (not as "%20")
      (str/replace "%2F" default-delimiter)
      (str/replace "%3D" "=")))                             ; TODO is this right? Can "=" be part of path?

(defn unescape-url ^String [^String s]
  (when (str/includes? s " ")
    (die (str "Can't unescape-url string containing space: " (pr-str s))))
  (URLDecoder/decode s "UTF-8"))

(defn ->URI          ^URI    [^String url] (rethrowing
                                             (str "Couldn't parse URL " (pr-str url))
                                             (let [fixed-url          (fix-url url)
                                                   normalized-uri     (.normalize (URI. fixed-url))
                                                   _                  (when-not (.getScheme normalized-uri)
                                                                        (die "Expected a scheme, e.g. fs://, but got none"))
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
(defn path-raw      ^String  [^String url] (.getRawPath (->URI url)))
(defn path          ^String  [^String url] (let [p (path-raw url)]
                                             (if (and (not (str/blank? p))
                                                      (not (re-matches pattern-url-no-auth-and-path url)))
                                               (unescape-url p))))
(defn filename      ^String  [^String url] (let [s (-> url path)
                                                 s (if (str/ends-with? s default-delimiter)
                                                     (subs s 0 (dec (count s)))
                                                     s)
                                                 f (subs s (inc (str/last-index-of s default-delimiter)))]
                                             (if-not (str/blank? f)
                                               f)))

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
(defmulti attrs   (fn [^String url & args] (scheme-k url)))    ; -> {:url ..., ...}
(defmulti copy    (fn [^String from-url ^String to-url & args] ; -> nil
                    (->URI from-url)                           ; ensure `from-url` is also parseable
                    (scheme-k to-url)))                        ; dispatch on `scheme` (and ensure it's also parseable)

(defmulti move    (fn [^String from-url ^String to-url & args] ; -> nil
                    (->URI from-url)                           ; ensure `from-url` is also parseable
                    (scheme-k to-url)))                        ; dispatch on `scheme` (and ensure it's also parseable)

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
        creds       (or (get config longest-url))           ; credentials (value) extracted by URL (key)
        c           (or config {})                          ; config -- for compatibility, credentials stored as keys
        e           (or env {})                             ; env    -- for compatibility, comes from JVM process (immutable, extracted as arg for testing)

        nie         (fn [s] (if (str/blank? s) nil s))      ; nil-if-empty

        ensure-url  (fn [k url-or-path]                     ; ensure-url
                      (cond (nil? url-or-path) nil
                            (str/starts-with? url-or-path default-delimiter) (str "file://" url-or-path)
                            (url? url-or-path) url-or-path
                            :else (die (str "Expected URL or path that starts with / for " k ", but got: " url-or-path))))

        creds       (if creds                               ; so it's the latest "url -> creds" version
                      creds
                      (case (scheme url)
                        ;        <current>             <obsolete>                    <obsolete>                  <obsolete>
                        "hdfs" {:principal     (or (c :hdfs.keytab.principal)    (e "HDFS_KEYTAB_PRINCIPAL") (e "KEYTAB_PRINCIPAL"))
                                :keytab        (or (c :hdfs.keytab.path)         (e "HDFS_KEYTAB_PATH")      (e "KEYTAB_FILE"))}

                        "sftp" {:user          (or (c :sftp.user)                (e "SFTP_USER")             (e "SSH_USER")              (die-creds-key-not-found :user        url creds))
                                :known-hosts   (or (c :sftp.known-hosts)         (e "SFTP_KNOWN_HOSTS")      (e "SSH_KNOWN_HOSTS")       (die-creds-key-not-found :known-hosts url creds))
                                :pass          (or (c :sftp.pass)                (e "SFTP_PASS")             (e "SSH_PASS"))
                                :identity      (or (c :sftp.identity)            (e "SFTP_IDENTITY")         (e "SSH_PRIVATE_KEY"))
                                :identity-pass (or (c :sftp.identity.pass)
                                                   (c :sftp.identity.passphrase) (e "SFTP_IDENTITY_PASS")    (e "SSH_PASSPHRASE"))}
                        {}))]
    ; TODO post-validate pairs?
    ; TODO fail on unknown keys in `cr`?

    ; if hdfs, replace empty strings with nil (required for proper work of HDFS API) + change path to URL
    (case (scheme url)
      "hdfs" (-> creds
                 (update :principal nie)
                 (update :keytab #(ensure-url :keytab (nie %))))
      creds)))

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
(defn try-with [url ->resource resouce->value close-resource]
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

(defn replace-path [^String url ^String absolute-path-or-blank]
  (let [u (->URI url)]
    (if (and (not (str/blank? absolute-path-or-blank))
             (not (str/starts-with? absolute-path-or-blank default-delimiter)))
      (die (str "Expected argument \"absolute-path-or-blank\" to start with a directory delimiter, but was: " (pr-str absolute-path-or-blank)))

      (str (.getScheme u)
           "://"
           (.getRawAuthority u)
           (escape-path (str absolute-path-or-blank))
           (when (.getRawQuery u)
             (str "?" (.getRawQuery u)))
           (when (.getRawFragment u)
             (str "#" (.getRawFragment u)))))))

(defn ends-with-delimiter? [url]
  (str/ends-with? (or (path url) "") default-delimiter))

(defn ensure-ends-with-delimiter [url]
  (if (ends-with-delimiter? url)
    url
    (str url default-delimiter)))

(defn ensure-not-ends-with-delimiter [^String url]
  (replace-path url
                (loop [p (path url)]
                  (if (and (str/ends-with? p default-delimiter)
                           (< 1 (count p)))
                    (recur (.substring p 0 (- (count p) 1)))
                    p))))

(defn with-parent [^String parent-url ^String file-unescaped]
  (if (str/includes? file-unescaped default-delimiter)
    (die (str "Expected argument \"file-unescaped\" to have no directory delimiters, but it had: " (pr-str file-unescaped))))
  (str (ensure-ends-with-delimiter parent-url)
       (escape-path file-unescaped)))

; Example:
; (parent-of "file:///path/to/file.txt") => "file:///path/to/"
; (parent-of "file:///path/to/")         => "file:///path/"
;
(defn parent-of                                             ; -> [String] (does not end with a slash)
  ([^String url] (parent-of default-delimiter url))
  ([^String delimiter ^String url]
   (let [p (path url)]                                      ; ensure it's a URL + compact possible trailing slashes into one
     (if (or (nil? p)                                       ; fs:// or fs:/// have no parent -- therefore nil
             (= delimiter p))
       nil
       (replace-path url
                     (let [p (if (str/ends-with? p delimiter)
                               (subs p 0 (dec (count p)))
                               p)
                           i (str/last-index-of p delimiter)
                           a (if i
                               (subs p 0 (inc i))
                               "")]
                       a))))))

(defn parents-between [parent-url child-url] ; => [{:url ... :dir true} ... ]
  (->> (iterate parent-of child-url)
       (drop 1)
       (take-while #(and (some? %)
                         (not (str/starts-with? parent-url %))))
       (map #(array-map :url % :dir true))
       (reverse)))

; See "test_uio.clj", (facts "intercalate-with-dirs works" ...)
(defn intercalate-with-dirs [viewed-from-url kvs]
  kvs
  (if (seq kvs)
    (concat (parents-between viewed-from-url (:url (first kvs)))
            [(first kvs)]
            (->> (partition 2 1 kvs)
                 (mapcat (fn [[a b]]
                           (concat (parents-between (:url a) (:url b))
                                   [b])))))))

; used by all `ls` implementations
(defmacro single-file-or [url & body]
  `(let [a# (attrs ~url)]
     (if (:size a#)
       [a#]
       ~@body)))

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
(defn url->seq-of-ext+s->s
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


(defn ^InputStream ->finalizing [^InputStream is]
  (Streams$FinalizingInputStream. is))

(defn ->buffered [is-os-r-or-w & [size]]
  (let [size (or size 8192)]
    (cond (instance? InputStream  is-os-r-or-w) (BufferedInputStream.  is-os-r-or-w size)
          (instance? OutputStream is-os-r-or-w) (BufferedOutputStream. is-os-r-or-w size)
          (instance? Reader       is-os-r-or-w) (BufferedReader.       is-os-r-or-w size)
          (instance? Writer       is-os-r-or-w) (BufferedWriter.       is-os-r-or-w size)
          :else                                           (die (str "Expected InputStream, OutputStream, Reader or Writer, but got: "
                                                                    (.getName (class is-os-r-or-w)))))))

; Count how many bytes came through a stream.
;
; Example:
; (with-open [is (counted-is (bytes->is (.getBytes "hello world")))]
;   (println (byte-count is))
;   (.read is)
;   (println (byte-count is)))
; 0
; 1
; => nil
; (with-open [os (counted-os nil)]
;   (println (byte-count os))
;   (.write os (.getBytes "hello world"))
;   (println (byte-count os)))
; 0
; 11
; => nil
;
; Example (advanced): measure compressed VS uncompressed ratio
; (with-open [os-file (->statsable (jio/output-stream (->nil-os)))
;             os-gz   (->statsable (encode :gz os-file))
;             w       (jio/writer os-gz)]
;
;   ; ==> (.write {Writer                           --> w
;   ;               {StatsableOutputStream          --> os-gz
;   ;                 {GZIPOutputStream
;   ;                   {StatsableOutputStream      --> os-file
;   ;                     {NullOutputStream}}}}}
;
;   (doseq [i (range 10000)]
;     (.write w (str i "\n")))
;
;   ; Close the outer os/writer to flush all underlying os/writers.
;   ; In this way we can get final counters, while still in `with-open` block.
;   (.close w)
;
;   {:original   (byte-count os-gz)
;    :compressed (byte-count os-file)})
;
;  => {:original 48890, :compressed 22196}
;
(defn ->statsable [^Closeable is-or-os]
  (cond (instance? InputStream  is-or-os) (Streams$StatsableInputStream. is-or-os)
        (instance? OutputStream is-or-os) (Streams$StatsableOutputStream. is-or-os)
        :else                             (die (str "Expected InputStream or OutputStream, but got "
                                                    (if (nil? is-or-os)
                                                      "nil"
                                                      (.getName (type is-or-os)))))))

(defn byte-count [^Streams$Statsable s]
  (.getByteCount s))

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
                            (url->seq-of-ext+s->s ext->is->is url))))

; TODO add examples
(defn ^OutputStream to* [^String url & args]
  (rethrowing (str "Couldn't apply os->os codecs to " (pr-str url))
              (apply-codecs (apply to (cons url args))
                            (url->seq-of-ext+s->s ext->os->os url))))

(defn ^InputStream concat-with [url->is urls]
  (Streams$ConcatInputStream. url->is urls))

(defn mkdirs-up-to [url mkdir-url->nil]
  (when-not (exists? url)                                   ; TODO refactor unnecessary extra (exists? ...)
    (->> (iterate parent-of url)
         (take-while some?)
         (take-while (complement exists?))
         (reverse)
         (#(if (empty? %)
             (die (str "Couldn't confirm existence of FS root to create directories up to: " (pr-str url)))
             %))
         (run! mkdir-url->nil))))

; Implementation: defaults
(defn default-impl [^String url ^String method args]
  (if (scheme url)
    (die (str "Method " (pr-str method) " is not implemented for " (pr-str url)))
    (die (str "Expected a URL with a scheme, but got: " (pr-str url) ". "
              "Available schemes are: "
              (->> method
                   symbol resolve deref methods keys
                   (remove #{:default})
                   (map #(str (name %) "://"))
                   sort
                   (str/join ", "))))))

(defmethod from    :default [url & args] (default-impl url "from"    args))
(defmethod to      :default [url & args] (default-impl url "to"      args))
(defmethod exists? :default [url & args] (default-impl url "exists?" args))
(defmethod delete  :default [url & args] (default-impl url "delete"  args))
(defmethod ls      :default [url & args] (default-impl url "ls"      args))
(defmethod mkdir   :default [url & args] (default-impl url "mkdir"   args))
(defmethod attrs   :default [url & args] (default-impl url "attrs"   args))

(defmethod copy    :default [from-url to-url & args] (with-open [is (from from-url)
                                                                 os (to to-url)]
                                                       (jio/copy is os :buffer-size 8192)))

; uses `attrs`, `copy`, `delete` and `mkdir` to move single files or dirs (recursively)
(defmethod move    :default [from-url to-url & args] (let [from-url-d (ensure-ends-with-delimiter from-url)
                                                           to-url-d   (ensure-ends-with-delimiter to-url)]

                                                       ; TODO prevent attempts of moving a parent dir into child
                                                       ; TODO get attrs of from-url and check whether it's a dir
                                                       ; TODO get attrs of to-url and if it exists, ensure it matches dir/file type of from-url
                                                       ;(when (str/starts-with? to-url from-url)
                                                       ;  (die (str "Can't move parent URL " (pr-str from-url)
                                                       ;            " to child URL " (pr-str to-url))))

                                                       (doseq [e (->> (ls from-url {:recurse true})
                                                                      (#(if (seq %)
                                                                          %
                                                                          (die-no-such-file from-url))))
                                                               :let [target-url (str to-url (subs (count from-url) (:url e)))]]
                                                         (cond (:size e) (do (copy from-url to-url)
                                                                             (delete from-url))
                                                               (:dir e)  (mkdir target-url)
                                                               :else     (die-no-such-file (:url e))))))

(defmethod size    :default [url & args] (let [as (attrs url)]
                                           (or (:size as)
                                               (if (:dir as)
                                                 (die-not-a-file url)
                                                 (die-no-such-file url)))))

(defmethod ext->is->is :default [_] nil)
(defmethod ext->os->os :default [_] nil)

; Example:
; (ns myns
;   (:require [uio.uio :as uio]      ; loads implementation
;             [uio.impl :as impl]))
;
; (uio.impl/list-avaimovelable-implementations)
; => {:fs     [:file :hdfs :http :https :mem :res :s3 :sftp]
;     :codecs [:bz2 :gz :xz]}
;
(defn list-available-implementations []
  {:fs     (-> (.getMethodTable from)        (dissoc :default) keys sort vec)
   :codecs (-> (.getMethodTable ext->is->is) (dissoc :default) keys sort vec)})

(defn close-when-realized-or-finalized [->close xs]
  (let [f (Streams$Finalizer. ->close)]
    (concat xs (lazy-seq (.close f)))))

; Call :default implementation of multimethod `m` with `args`
(defn invoke-default [^MultiFn m & args]
  (apply (or (:default (.getMethodTable m))
             (die (str "Couldn't find implementation :default of multimethod " m)))
         args))
