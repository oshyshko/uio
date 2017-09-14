(ns uio.test-java-api
    (:require [midje.sweet :refer :all])
    (:import [uio Uio]
             [java.nio.file Paths]))

(facts "Java API works"
    (spit  (Uio/to   (str (.toUri (Paths/get "target/temp.txt" (into-array String []))))) "test data")
    (slurp (Uio/from (str (.toUri (Paths/get "target/temp.txt" (into-array String [])))))) => "test data"

    ; TODO add tests for other methods
    )