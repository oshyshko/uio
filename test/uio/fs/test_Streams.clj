(ns uio.fs.test-Streams
  (:require [midje.sweet :refer :all]
            [uio.impl :as impl])
  (:import [uio.fs Streams$TakeNInputStream]))

(facts "Streams$TakeNInputStream works"
  (->> (impl/bytes->is (.getBytes "hello world"))
       (Streams$TakeNInputStream. 5)
       slurp)
  => "hello")
