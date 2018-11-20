(ns uio.fs.test-s3
  (:require [uio.impl :refer :all]
            [uio.fs.s3 :refer :all]
            [midje.sweet :refer :all]))

(facts "path-no-slash works"
  (url->key "foo://user@host:8080/some-dir/file.txt?arg=value") => "some-dir/file.txt"

  (url->key "s3://bucket")       => ""
  (url->key "s3://bucket/")      => ""
  (url->key "s3://bucket//")     => ""
  (url->key "s3://bucket//asd")  => "asd"
  (url->key "s3://bucket///asd") => "asd"

  (bucket-key->url "bucket" "key")         => "s3://bucket/key"
  (bucket-key->url "bucket" "1 + %20.txt") => "s3://bucket/1+%2B+%2520.txt")
