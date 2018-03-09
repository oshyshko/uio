(ns uio.test-uio
  (:require [uio.uio :refer :all]
            [uio.impl :refer [ensure-not-ends-with-delimiter
                              creds-url->creds
                              intercalate-with-dirs
                              longest-matching-prefix
                              replace-path
                              scheme-k
                              url->creds'
                              url->seq-of-ext+s->s
                              ->URI]]
            [midje.sweet :refer :all])
  (:import (java.util.zip GZIPOutputStream)
           (org.apache.commons.compress.compressors CompressorStreamFactory)))

(facts "URL manipulation fns are working"
  (-> "file:///Virtual+Box/gentoo.vdi" ->URI .getRawPath)        => "/Virtual%20Box/gentoo.vdi"
  (-> "file:///Virtual+Box/gentoo.vdi" path)                     => "/Virtual Box/gentoo.vdi"
  (-> "file:///Virtual+Box/gentoo+%25.vdi" path)                 => "/Virtual Box/gentoo %.vdi"

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

  (path      "foo://host/+ %20")                                 => (throws #"Illegal character in path")
  (path      "file:///path/to/file+1%2B%25.txt")                 => "/path/to/file 1+%.txt"

  (filename "file:///path/to/file+1%2B%25.txt")                  => "file 1+%.txt"
  (filename "file:///path/to")                                   => "to"
  (filename "file:///path/to/")                                  => nil

  (query     "foo://")                                           => nil
  (query     "foo:///")                                          => nil
  (query     "foo://user@host:8080/some-dir/file.txt?arg=value") => "arg=value"
  (query     "foo://host/file.txt?arg=value&arg2=+:/")           => "arg=value&arg2=%20:/"

  (query-map "foo://")                                           => {}
  (query-map "foo:///")                                          => {}
  (query-map "foo://user@host:8080?k=v&k2=v1&k2=v2&no-value=&=no-key&encoded%3Dkey%25=encoded%26value+%20%25")
                                                                 => {:k            ["v"]
                                                                     :k2           ["v1" "v2"]
                                                                     :no-value     []
                                                                     nil           ["no-key"]
                                                                     :encoded=key% ["encoded&value  %"]}

  (but-query "foo://")                                           => "foo://"
  (but-query "foo:///")                                          => "foo:///"
  (but-query "foo://user@host:8090?k=v&k2=v1&k2=v2&no-value=&=no-key&encoded%3Dkey=encoded%26value")
                                                                 => "foo://user@host:8090"

  (parent-of "file:///path/to/file.txt")                         => "file:///path/to/"
  (parent-of "file:///path/to/")                                 => "file:///path/"
  (parent-of "file:///path/")                                    => "file:///"
  (parent-of "file:///path")                                     => "file:///"
  (parent-of "file:///")                                         => nil
  (parent-of "file://")                                          => nil
  
  (parent-of "/path/to/file.txt")                                => (throws #"Expected a scheme")
  (parent-of "path/to/file.txt")                                 => (throws #"Expected a scheme")

  (parent-of "file:///path/to/file+1%2B%25.txt")                 => "file:///path/to/"

  (with-parent "file://path/to/" "file 1+%.txt")                 => "file://path/to/file+1%2B%25.txt"
  (with-parent "file:///" "file 1+%.txt")                        => "file:///file+1%2B%25.txt"

  (normalize "file://")                                          => "file:///"
  (normalize "file:///")                                         => "file:///"
  (normalize "file:////")                                        => "file:///"
  (normalize "file://///")                                       => "file:///"
  (normalize "file://host/path/to")                              => "file://host/path/to"
  (normalize "file://host/path/to/")                             => "file://host/path/to/"
  (normalize "file://host/path/to//")                            => "file://host/path/to/"
  (normalize "file:///path/to//")                                => "file:///path/to/"

  (replace-path "fs:///path/to/file+1%2B%25.txt" "/path/to/")    => "fs:///path/to/"
  (replace-path "fs://host/path/to/file.txt" "/")                => "fs://host/"
  (replace-path "fs://user@host:123/path/to/file.txt?a=b#frag"
                "/another/path/to/file.txt")                     => "fs://user@host:123/another/path/to/file.txt?a=b#frag"

  (replace-path "fs:///" nil)                                    => "fs://"
  (replace-path "fs:/" nil)                                      => "fs://"
  (replace-path "fs:" nil)                                       => (throws #"Couldn't parse URL")
  (replace-path "fs:///" "/path")                                => "fs:///path"
  (replace-path "fs:///1+%20?+=%20#%20" "/path/1 +%.txt")        => "fs:///path/1+%2B%25.txt?%20=%20#%20"

  (replace-path "fs://user@host:123/path/to/file.txt" "")        => "fs://user@host:123"
  (replace-path "fs://user@host:123/path/to/file.txt" nil)       => "fs://user@host:123"
  (replace-path "fs://user@host:123/path/to/file.txt?a=b" "")    => "fs://user@host:123?a=b"
  (replace-path "fs://host/path/to/file.txt" "file.txt")         => (throws #"Expected argument")
  
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

             "s3://"   {:access   "access-c11"
                        :secret   "secret-c11"
                        :role-arn "role-arn-c11"}

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

                 "s3://"   {:access   "access-c10"
                            :secret   "secret-c10"
                            :role-arn nil}

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

                  "s3://"   {:access   "access-e10"
                             :secret   "secret-e10"
                             :role-arn nil}

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

                  "s3://"   {:access   "access-e09"
                             :secret   "secret-e09"
                             :role-arn nil}

                  "sftp://" {:user          "user-e09"
                             :known-hosts   "known-hosts-e09"
                             :pass          "pass-e09"
                             :identity      "identity-e09"
                             :identity-pass "identity-pass-e09"}}]

      (url->creds' {} e09 "hdfs://") => (cr-e09 "hdfs://")
      (url->creds' {} e09 "s3://")   => (cr-e09 "s3://")
      (url->creds' {} e09 "sftp://") => (cr-e09 "sftp://")) ))

(facts "Deducing of (de)compression codecs works, even for chained ones"
  (map first (url->seq-of-ext+s->s ext->is->is "hdfs:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->seq-of-ext+s->s ext->os->os "sftp:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->seq-of-ext+s->s ext->os->os "mem:///dont-forget-leading-slash.txt.gz"))      => [:gz]

  ; unknown codecs between known ones
  (url->seq-of-ext+s->s ext->is->is "hdfs:///far-away/and/well-archived.xz.ufoz.bz2.gz") => (throws Exception #"Got at least one unsupported codec")
  (url->seq-of-ext+s->s ext->os->os "sftp:///far-away/and/well-archived.xz.ufoz.bz2.gz") => (throws Exception #"Got at least one unsupported codec"))

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
  (intercalate-with-dirs "fs:///"[])                          => nil

  ; base case + 1
  (intercalate-with-dirs "fs:///"
                         [{:url "fs:///1.txt" :size 1}])      => [{:url "fs:///1.txt" :size 1}]

  ; base case + 2
  (intercalate-with-dirs "fs:///"
                         [{:url "fs:///1.txt"     :size 1}
                          {:url "fs:///a/b/2.txt" :size 2}])  => [{:url "fs:///1.txt" :size 1}
                                                                  {:url "fs:///a/" :dir true}
                                                                  {:url "fs:///a/b/" :dir true}
                                                                  {:url "fs:///a/b/2.txt" :size 2}]

  (intercalate-with-dirs "fs:///"
                         [{:url "fs:///a/1.txt" :size 1}
                          {:url "fs:///b/2.txt" :size 2}])    => [{:url "fs:///a/" :dir true}
                                                                  {:url "fs:///a/1.txt" :size 1}
                                                                  {:url "fs:///b/" :dir true}
                                                                  {:url "fs:///b/2.txt" :size 2}]
  ; base case + 2
  (intercalate-with-dirs "fs:///"
                         [{:url "fs:///1.txt"   :size 1}
                          {:url "fs:///a/2.txt" :size 2}])    => [{:url "fs:///1.txt" :size 1}
                                                                  {:url "fs:///a/" :dir true}
                                                                  {:url "fs:///a/2.txt" :size 2}]
  ; complex case
  (intercalate-with-dirs "fs:///"
                         [{:url "fs:///1.txt"         :size 1}
                          {:url "fs:///123.txt"       :size 2}
                          ; 123
                          {:url "fs:///123/1.txt"     :size 3}
                          {:url "fs:///123/2.txt"     :size 4}
                          {:url "fs:///123/3.txt"     :size 5}
                          ; 123/123
                          {:url "fs:///123/123/1.txt" :size 6}
                          ; 456
                          {:url "fs:///456/1.txt"     :size 7}
                          ; 456/123
                          {:url "fs:///456/123/1.txt" :size 8}
                          {:url "fs:///456/123/2.txt" :size 9}
                          ; 456/456
                          {:url "fs:///456/456/3.txt" :size 10}
                          {:url "fs:///456/5.txt"     :size 11}
                          {:url "fs:///789.txt"       :size 12}]) => [{:url "fs:///1.txt"         :size 1}
                                                                      {:url "fs:///123.txt"       :size 2}
                                                                      {:url "fs:///123/"          :dir true}
                                                                      {:url "fs:///123/1.txt"     :size 3}
                                                                      {:url "fs:///123/2.txt"     :size 4}
                                                                      {:url "fs:///123/3.txt"     :size 5}
                                                                      {:url "fs:///123/123/"      :dir true}
                                                                      {:url "fs:///123/123/1.txt" :size 6}
                                                                      {:url "fs:///456/"          :dir true}
                                                                      {:url "fs:///456/1.txt"     :size 7}
                                                                      {:url "fs:///456/123/"      :dir true}
                                                                      {:url "fs:///456/123/1.txt" :size 8}
                                                                      {:url "fs:///456/123/2.txt" :size 9}
                                                                      {:url "fs:///456/456/"      :dir true}
                                                                      {:url "fs:///456/456/3.txt" :size 10}
                                                                      {:url "fs:///456/5.txt"     :size 11}
                                                                      {:url "fs:///789.txt"       :size 12}]
  )

(facts "encode + decode works"
  (let [s "hello world"]
    (->> s (.getBytes) (encode :gz)  (decode :gz)  (String.) ) => s
    (->> s (.getBytes) (encode :bz2) (decode :bz2) (String.) ) => s
    (->> s (.getBytes) (encode :xz)  (decode :xz)  (String.) ) => s

    ; check built-in filters
    (map #(count (encode % (.getBytes s)))
         [:gz :bz2 :xz]) => [31 48 68]))

(facts "escape-url + unescape-url works"
  (escape-url   "1 +%")     => "1+%2B%25"
  (unescape-url "1+%2B%25") => "1 +%"
  (unescape-url "1 +%20")   => (throws #"Can't unescape-url string containing space"))
