(ns uio.fs.test-s3
  (:require [uio.impl :refer :all]
            [uio.fs.s3 :refer :all]
            [midje.sweet :refer :all]))

(facts "path-no-slash works"
  (path-no-leading-slash "foo://user@host:8080/some-dir/file.txt?arg=value") => "some-dir/file.txt"

  (path-no-leading-slash "s3://bucket")       => ""
  (path-no-leading-slash "s3://bucket/")      => ""
  (path-no-leading-slash "s3://bucket//")     => ""
  (path-no-leading-slash "s3://bucket//asd")  => "asd"
  (path-no-leading-slash "s3://bucket///asd") => "asd")
