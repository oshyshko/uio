(ns uio.test-uio
  (:require [uio.uio :refer :all]
            [uio.impl :refer [url->ext+s->s intercalate-with-dirs ensure-has-no-trailing-slash]]
            [midje.sweet :refer :all])
  (:import (java.util.zip GZIPOutputStream)
           (org.apache.commons.compress.compressors CompressorStreamFactory)))

(facts "URL manipulation fns are working"
  (scheme        "foo://user@host:8080/some-dir/file.txt?arg=value") => :foo
  (host          "foo://user@host:8080/some-dir/file.txt?arg=value") => "host"
  (port          "foo://user@host:8080/some-dir/file.txt?arg=value") => 8080
  (path          "foo://user@host:8080/some-dir/file.txt?arg=value") => "/some-dir/file.txt"
  (path-no-slash "foo://user@host:8080/some-dir/file.txt?arg=value") => "some-dir/file.txt"

  (ensure-has-no-trailing-slash "test")                              => "test"
  (ensure-has-no-trailing-slash "test/")                             => "test"
  (ensure-has-no-trailing-slash "test///")                           => "test")

(facts "In-memory implementation works"
  (spit  (to   "mem:///greeetings.txt") "hello") => nil
  (slurp (from "mem:///greeetings.txt"))         => "hello")

(facts "Deducing of (de)compression codecs works, even for chained ones"
  (map first (url->ext+s->s ext->is->is "hdfs:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->ext+s->s ext->os->os "sftp:///far-away/and/well-archived.xz.bz2.gz")) => [:gz :bz2 :xz]
  (map first (url->ext+s->s ext->os->os "mem:///dont-forget-leading-slash.txt.gz"))   => [:gz]

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
                                                             {:url "123" :dir true}
                                                             {:url "123/2.txt"}]
  ; simple case + continue + skip matching last flushed dir
  (intercalate-with-dirs "123" [{:url "1.txt"}
                                {:url "123/2.txt"}]) => [{:url "1.txt"}
                                                         {:url "123/2.txt"}]
  ; simple case + continue
  (intercalate-with-dirs "123" [{:url "456/1.txt"}
                                {:url "456/2.txt"}]) => [{:url "456" :dir true}
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
                                                         {:url "123" :dir true}
                                                         {:url "123/1.txt"}
                                                         {:url "123/2.txt"}
                                                         {:url "123/3.txt"}
                                                         {:url "123/123" :dir true}
                                                         {:url "123/123/1.txt"}
                                                         {:url "456" :dir true}
                                                         {:url "456/1.txt"}
                                                         {:url "456/123" :dir true}
                                                         {:url "456/123/1.txt"}
                                                         {:url "456/123/2.txt"}
                                                         {:url "456/456" :dir true}
                                                         {:url "456/456/3.txt"}
                                                         {:url "456/5.txt"}
                                                         {:url "789.txt"}])

; TODO convert this to an intergtation test (should not be executed with `lein install` or `deploy`)
;
;(let [content  "hello world!\nnext line\n"
;      temp-url "file:///tmp/uio-demo-temp.txt"]
;
;  (doseq [url ["file:///tmp/uio-demo.txt"
;               "hdfs:///tmp/uio-demo.txt"
;               "s3://kevint.test/tmp/uio-demo.txt"
;               "http://www.google.com"]]
;
;    (println "Testing" (keyword (scheme url)) "...")
;
;    (spit (path temp-url) content)                          ; setup
;    (if (exists? url)
;      (delete url))
;
;    (with-open [os (to url)]                                ; to / spit TODO test overwrite + permissions?
;      (assert (exists? url))
;      (assert (instance? OutputStream os))
;      (spit os content))
;
;    (with-open [is (from url)]                              ; from / slurp TODO test non-existent + permissions?
;      (assert (instance? InputStream is))
;      (assert (= content (slurp is))))
;
;    (assert (exists? url))                                  ; delete
;    (assert (nil? (delete url)))
;    (assert (not (exists? url)))
;    ;(assert (zero? (size url)))                            ; TODO assert throws exception
;
;    (assert (nil? (copy temp-url url)))                     ; copy
;    (assert (exists? url))
;    (assert (= (count content) (size url)))
;    (with-open [is (from url)]
;      (assert (instance? InputStream is))
;      (assert (= content (slurp is))))
;
;    (assert (nil? (delete url)))))
