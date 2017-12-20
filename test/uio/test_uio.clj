(ns uio.test-uio
  (:require [uio.uio :refer :all]
            [uio.impl :refer [ensure-not-ends-with-delimiter
                              creds-url->creds
                              intercalate-with-dirs
                              longest-matching-prefix
                              replace-path
                              scheme-k
                              url->creds'
                              url->ext+s->s]]
            [midje.sweet :refer :all])
  (:import (java.util.zip GZIPOutputStream)
           (org.apache.commons.compress.compressors CompressorStreamFactory)))

(facts "URL manipulation fns are working"
  (scheme    "foo://user@host:8080/some-dir/file.txt?arg=value") => "foo"
  (scheme-k  "foo://user@host:8080/some-dir/file.txt?arg=value") => :foo

  (user      "foo:///")                                          => nil
  (user      "foo://?k=v")                                       => nil
  (user      "foo://user@host:8080/some-dir/file.txt?arg=value") => "user"

  (host      "foo://")                                           => nil
  (host      "foo://?k=v")                                       => nil
  (host      "foo:///")                                          => nil
  (host      "foo://user@host:8080/some-dir/file.txt?arg=value") => "host"

  (path      "foo://")                                           => nil
  (path      "foo://?k=v")                                       => nil
  (path      "foo:///")                                          => "/"
  (path      "foo://host")                                       => nil
  (port      "foo://user@host:8080/some-dir/file.txt?arg=value") => 8080
  (port      "foo://user@host/some-dir/file.txt?arg=value")      => nil
  (path      "foo://user@host:8080/some-dir/file.txt?arg=value") => "/some-dir/file.txt"

  (query     "foo://")                                           => nil
  (query     "foo:///")                                          => nil
  (query     "foo://user@host:8080/some-dir/file.txt?arg=value") => "arg=value"

  (query-map "foo://")                                           => {}
  (query-map "foo:///")                                          => {}
  (query-map "foo://user@host:8080?k=v&k2=v1&k2=v2&no-value=&=no-key&encoded%3Dkey=encoded%26value")
                                                                 => {:k           ["v"]
                                                                     :k2          ["v1" "v2"]
                                                                     :no-value    []
                                                                     nil          ["no-key"]
                                                                     :encoded=key ["encoded&value"]}

  (but-query "foo://")                                           => "foo://"
  (but-query "foo:///")                                          => "foo:///"
  (but-query "foo://user@host:8090?k=v&k2=v1&k2=v2&no-value=&=no-key&encoded%3Dkey=encoded%26value")
                                                                 => "foo://user@host:8090"

  (parent-of "file:///path/to/file.txt")                         => "file:///path/to/"
  (parent-of "/path/to/file.txt")                                => "/path/to/"
  (parent-of "path/to/file.txt")                                 => "path/to/"

  (normalize "file://")                                          => "file:///"
  (normalize "file:///")                                         => "file:///"
  (normalize "file:////")                                        => "file:///"
  (normalize "file://///")                                       => "file:///"
  (normalize "file://host/path/to")                              => "file://host/path/to"
  (normalize "file://host/path/to/")                             => "file://host/path/to/"
  (normalize "file://host/path/to//")                            => "file://host/path/to/"
  (normalize "file:///path/to//")                                => "file:///path/to/"

  (replace-path "sftp://host/path/to/file.txt" "file.txt")       => (throws #"Expected argument")
  (replace-path "sftp://host/path/to/file.txt" "/")              => "sftp://host/"

  (replace-path "sftp://user@host:123/path/to/file.txt" "")      => (throws #"Expected argument")
  (replace-path "sftp://user@host:123/path/to/file.txt"
                "/another/path/to/file.txt")                     => "sftp://user@host:123/another/path/to/file.txt"

  (ensure-not-ends-with-delimiter "test")                        => "test"
  (ensure-not-ends-with-delimiter "test/")                       => "test"
  (ensure-not-ends-with-delimiter "test///")                     => "test")

(facts "longest-matching-prefix works"
  (let [prefixes ["fs://"
                  "fs://host/path/to/file.txt"
                  "fs://user@host/"
                  "fs://user@host:port/path/"
                  "fs://user@host:port/path/to/file.txt"]]

    (longest-matching-prefix []       "fs://host/path/to/file.txt"          ) => nil
    (longest-matching-prefix prefixes "fs://"                               ) => "fs://"
    (longest-matching-prefix prefixes "fs://abcd"                           ) => "fs://"
    (longest-matching-prefix prefixes "fs://host/path/to/file.txt"          ) => "fs://host/path/to/file.txt"
    (longest-matching-prefix prefixes "fs://user@host/some-file.txt"        ) => "fs://user@host/"
    (longest-matching-prefix prefixes "fs://user@host:port/path/to/file.txt") => "fs://user@host:port/path/to/file.txt"
    (longest-matching-prefix prefixes "unknownfs://host/path/to/file.txt")    => nil))

(facts "creds-url->creds"
  (creds-url->creds "fs://host?user=joe&pass=secret")          => {"fs://host"          {:user "joe" :pass "secret"}}
  (creds-url->creds "fs://host/home/joe?user=joe&pass=secret") => {"fs://host/home/joe" {:user "joe" :pass "secret"}}
  (creds-url->creds "fs://host?user=joe&user=sarah")           => (throws "Got multiple \"user\" keys in credentials URL starting with: fs://host")
  (creds-url->creds "fs://host?=joe")                          => (throws "Got an empty key in credentials URL starting with: fs://host"))

(facts "url->creds works"
  ; TODO understand these old tests
  ;(url->creds' {:config {}
  ;              :env   {}}
  ;             "s3://bucket/path/to/file.txt") => nil
  ;
  ;(url->creds' {:config {"s3://" {:origin "new-config"
  ;                                :access "new-config"
  ;                                :secret "new-config"}}
  ;              :env   {}}
  ;             "s3://bucket/path/to/file.txt") => {:origin "new-config"
  ;                                                 :access "from-config"
  ;                                                 :secret "from-config"}

  (let [c11 {"hdfs://" {:principal "principal-c11"          ; v1.1
                        :keytab    "file:///path/to/keytab-c11"
                        :access    "access-c11"
                        :secret    "secret-c11"}

             "s3://"   {:access "access-c11"
                        :secret "secret-c11"}

             "sftp://" {:user          "user-c11"
                        :known-hosts   "known-hosts-c11"
                        :pass          "pass-c11"
                        :identity      "identity-c11"
                        :identity-pass "identity-pass-c11"}}

        c10 {:hdfs.keytab.principal "principal-c10"         ; v1.0
             :hdfs.keytab.path      "/path/to/keytab-c10"

             :s3.access             "access-c10"
             :s3.secret             "secret-c10"

             :sftp.user             "user-c10"
             :sftp.known-hosts      "known-hosts-c10"
             :sftp.pass             "pass-c10"
             :sftp.identity         "identity-c10"
             :sftp.identity.pass    "identity-pass-c10"}

        e10 {"HDFS_KEYTAB_PRINCIPAL" "principal-e10"        ; v1.0
             "HDFS_KEYTAB_PATH"      "/path/to/keytab-e10"

             "AWS_ACCESS"            "access-e10"
             "AWS_SECRET"            "secret-e10"

             "SFTP_USER"             "user-e10"
             "SFTP_KNOWN_HOSTS"      "known-hosts-e10"
             "SFTP_PASS"             "pass-e10"
             "SFTP_IDENTITY"         "identity-e10"
             "SFTP_IDENTITY_PASS"    "identity-pass-e10"}

        e09 {"KEYTAB_PRINCIPAL"      "principal-e09"        ; v0.9
             "KEYTAB_FILE"           "/path/to/keytab-e09"

             "AWS_ACCESS_KEY_ID"     "access-e09"
             "AWS_SECRET_ACCESS_KEY" "secret-e09"

             "SSH_USER"              "user-e09"
             "SSH_KNOWN_HOSTS"       "known-hosts-e09"
             "SSH_PASS"              "pass-e09"
             "SSH_PRIVATE_KEY"       "identity-e09"
             "SSH_PASSPHRASE"        "identity-pass-e09"}]

    ; c11 works without env and beats c10, e10 and e09
    (let[ cr-c11 c11]
      (url->creds' c11 {} "hdfs://")             => (cr-c11 "hdfs://")
      (url->creds' c11 {} "s3://")               => (cr-c11 "s3://")
      (url->creds' c11 {} "sftp://")             => (cr-c11 "sftp://")

      (url->creds' (merge c11 c10) {} "hdfs://") => (cr-c11 "hdfs://")
      (url->creds' (merge c11 c10) {} "s3://")   => (cr-c11 "s3://")
      (url->creds' (merge c11 c10) {} "sftp://") => (cr-c11 "sftp://")

      (url->creds' c11 e09 "hdfs://")            => (cr-c11 "hdfs://")
      (url->creds' c11 e09 "s3://")              => (cr-c11 "s3://")
      (url->creds' c11 e09 "sftp://")            => (cr-c11 "sftp://")

      (url->creds' c11 e10 "hdfs://")            => (cr-c11 "hdfs://")
      (url->creds' c11 e10 "s3://")              => (cr-c11 "s3://")
      (url->creds' c11 e10 "sftp://")            => (cr-c11 "sftp://"))

    ; c10 works without env and beats e10 and e09
    (let[cr-c10 {"hdfs://" {:principal "principal-c10"
                            :keytab    "file:///path/to/keytab-c10"
                            :access    "access-c10"
                            :secret    "secret-c10"}

                 "s3://"   {:access "access-c10"
                            :secret "secret-c10"}

                 "sftp://" {:user          "user-c10"
                            :known-hosts   "known-hosts-c10"
                            :pass          "pass-c10"
                            :identity      "identity-c10"
                            :identity-pass "identity-pass-c10"}}]

      (url->creds' c10 {} "hdfs://")  => (cr-c10 "hdfs://")
      (url->creds' c10 {} "s3://")    => (cr-c10 "s3://")
      (url->creds' c10 {} "sftp://")  => (cr-c10 "sftp://")

      (url->creds' c10 e09 "hdfs://") => (cr-c10 "hdfs://")
      (url->creds' c10 e09 "s3://")   => (cr-c10 "s3://")
      (url->creds' c10 e09 "sftp://") => (cr-c10 "sftp://")

      (url->creds' c10 e10 "hdfs://") => (cr-c10 "hdfs://")
      (url->creds' c10 e10 "s3://")   => (cr-c10 "s3://")
      (url->creds' c10 e10 "sftp://") => (cr-c10 "sftp://"))

    ; e10 works without config and beats e09
    (let [cr-e10 {"hdfs://" {:principal "principal-e10"
                             :keytab    "file:///path/to/keytab-e10"
                             :access    "access-e10"
                             :secret    "secret-e10"}

                  "s3://"   {:access "access-e10"
                             :secret "secret-e10"}

                  "sftp://" {:user          "user-e10"
                             :known-hosts   "known-hosts-e10"
                             :pass          "pass-e10"
                             :identity      "identity-e10"
                             :identity-pass "identity-pass-e10"}}]

      (url->creds' {} e10 "hdfs://")              => (cr-e10 "hdfs://")
      (url->creds' {} e10 "s3://")                => (cr-e10 "s3://")
      (url->creds' {} e10 "sftp://")              => (cr-e10 "sftp://")

      (url->creds' {} (merge e10 e09) "hdfs://")  => (cr-e10 "hdfs://")
      (url->creds' {} (merge e10 e09) "s3://")    => (cr-e10 "s3://")
      (url->creds' {} (merge e10 e09) "sftp://")  => (cr-e10 "sftp://"))

    ; e09 works without config
    (let [cr-e09 {"hdfs://" {:principal "principal-e09"
                             :keytab    "file:///path/to/keytab-e09"
                             :access    "access-e09"
                             :secret    "secret-e09"}

                  "s3://"   {:access "access-e09"
                             :secret "secret-e09"}

                  "sftp://" {:user          "user-e09"
                             :known-hosts   "known-hosts-e09"
                             :pass          "pass-e09"
                             :identity      "identity-e09"
                             :identity-pass "identity-pass-e09"}}]

      (url->creds' {} e09 "hdfs://") => (cr-e09 "hdfs://")
      (url->creds' {} e09 "s3://")   => (cr-e09 "s3://")
      (url->creds' {} e09 "sftp://") => (cr-e09 "sftp://")) ))

(facts "In-memory implementation works"
  (spit  (to   "mem:///greeetings.txt") "hello") => nil
  (slurp (from "mem:///greeetings.txt"))         => "hello")

(facts "Deducing of (de)compression codecs works, even for chained ones"
  (map first (url->ext+s->s ext->is->is "hdfs:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->ext+s->s ext->os->os "sftp:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->ext+s->s ext->os->os "mem:///dont-forget-leading-slash.txt.gz"))      => [:gz]

  ; unknown codecs between known ones
  (url->ext+s->s ext->is->is "hdfs:///far-away/and/well-archived.xz.ufoz.bz2.gz") => (throws Exception #"Got at least one unsupported codec")
  (url->ext+s->s ext->os->os "sftp:///far-away/and/well-archived.xz.ufoz.bz2.gz") => (throws Exception #"Got at least one unsupported codec"))

(facts "(De)compression works, even for chained extensions"
  (dorun
    (map (fn [[url content os->compressing-os]]
           (spit (to* url) content)                         ; put

           (seq (is->bytes (from url)))                     ; ensure that stored content was actually compressed
           => (seq (with-baos->bytes #(with-open [os (os->compressing-os %)]
                                        (.write os (.getBytes content)))))

           (slurp (from* url)) => content)                  ; get

         ; [ [url content os->compressing-os]
         [["mem:///file.txt"            "I am plain text"   identity]
          ["mem:///file.txt.gz"         "I am gzipped text" #(GZIPOutputStream. %)]
          ["mem:///file.txt.bz2"        "I am bzipped text" #(.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/BZIP2 %)]
          ["mem:///file.txt.xz"         "I am xzipped text" #(.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/XZ %)]
          ["mem:///file1.txt.xz.bz2.gz" "I am xzipped, bzipped and gzipped text"
           #(->> (GZIPOutputStream. %)
                 (.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/BZIP2)
                 (.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/XZ))]])))

(facts "intercalate-with-dirs works"
  ; base case
  (intercalate-with-dirs [])                         => []

  ; base case + 1
  (intercalate-with-dirs [{:url "1.txt"}])           => [{:url "1.txt"}]

  ; base case + 2 + flush
  (intercalate-with-dirs [{:url "1.txt"}
                          {:url "123/2.txt"}])       => [{:url "1.txt"}
                                                         {:url "123/" :dir true}
                                                         {:url "123/2.txt"}]
  ; simple case + continue + skip matching last flushed dir
  (intercalate-with-dirs "123/" [{:url "1.txt"}
                                 {:url "123/2.txt"}]) => [{:url "1.txt"}
                                                          {:url "123/2.txt"}]
  ; simple case + continue
  (intercalate-with-dirs "123/" [{:url "456/1.txt"}
                                 {:url "456/2.txt"}]) => [{:url "456/" :dir true}
                                                          {:url "456/1.txt"}
                                                          {:url "456/2.txt"}]
  ; complex case
  (intercalate-with-dirs [{:url "1.txt"}
                          {:url "123.txt"}
                          ; 123
                          {:url "123/1.txt"}
                          {:url "123/2.txt"}
                          {:url "123/3.txt"}
                          ; 123/123
                          {:url "123/123/1.txt"}
                          ; 456
                          {:url "456/1.txt"}
                          ; 456/123
                          {:url "456/123/1.txt"}
                          {:url "456/123/2.txt"}
                          ; 456/456
                          {:url "456/456/3.txt"}
                          {:url "456/5.txt"}
                          {:url "789.txt"}])         => [{:url "1.txt"}
                                                         {:url "123.txt"}
                                                         {:url "123/"     :dir true}
                                                         {:url "123/1.txt"}
                                                         {:url "123/2.txt"}
                                                         {:url "123/3.txt"}
                                                         {:url "123/123/" :dir true}
                                                         {:url "123/123/1.txt"}
                                                         {:url "456/"     :dir true}
                                                         {:url "456/1.txt"}
                                                         {:url "456/123/" :dir true}
                                                         {:url "456/123/1.txt"}
                                                         {:url "456/123/2.txt"}
                                                         {:url "456/456/" :dir true}
                                                         {:url "456/456/3.txt"}
                                                         {:url "456/5.txt"}
                                                         {:url "789.txt"}])

(facts "encode + decode works"
  (let [s "hello world"]
    (->> s (.getBytes) (encode :gz)  (decode :gz)  (String.) ) => s
    (->> s (.getBytes) (encode :bz2) (decode :bz2) (String.) ) => s
    (->> s (.getBytes) (encode :xz)  (decode :xz)  (String.) ) => s

    ; check built-in filters
    (map #(count (encode % (.getBytes s)))
         [:gz :bz2 :xz]) => [31 48 68]))
