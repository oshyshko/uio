(ns uio.fs.test-s3
  (:require [uio.impl :refer :all]
            [uio.fs.s3 :refer :all]
            [midje.sweet :refer :all]))

(facts "path-no-slash works"
  (path-no-slash "foo://user@host:8080/some-dir/file.txt?arg=value")
  => "some-dir/file.txt")