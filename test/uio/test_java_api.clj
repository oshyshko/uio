(ns uio.test-java-api
  (:require [midje.sweet :refer :all]
            [uio.uio :as uio]
            [uio.fs.mem :as mem])
  (:import [uio Uio]
           [java.nio.file Paths]))

(facts "Java API works"
  (spit (Uio/to (str (.toUri (Paths/get "target/temp.txt" (into-array String []))))) "test data")
  (slurp (Uio/from (str (.toUri (Paths/get "target/temp.txt" (into-array String [])))))) => "test data"

  ; test mem + Java API, that there's no class cast exceptions with Integer vs Long
  (mem/reset)

  (spit (uio/to "mem:///1.txt") "hello")
  (map bean (Uio/ls "mem:///")) => [{:url   "mem:///1.txt"
                                     :size  5
                                     :class uio.Uio$Entry
                                     :dir   false
                                     :file  true
                                     :extra {}}])
