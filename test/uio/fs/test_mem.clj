(ns uio.fs.test-mem
  (:require [uio.fs.mem :as mem]
            [uio.impl :refer :all]
            [midje.sweet :refer :all]))

(facts "Mem works"
  (mem/reset)

  (spit (to "mem:///123/456.txt")       "a")
  (spit (to "mem:///123/456/aa.txt")    "bb")
  (spit (to "mem:///123/456/aa/bb.txt") "ccc")
  (spit (to "mem:///123/456/cc.txt")    "dddd")

  (ls "mem:///")        => [{:url "mem:///123"               :dir  true}]

  (ls "mem:///123/")    => (ls "mem:///123/")
  (ls "mem:///123")     => [{:url "mem:///123/456.txt"       :size 1}
                            {:url "mem:///123/456"           :dir  true}]


  (ls "mem:///123/456") => [{:url "mem:///123/456/aa.txt"    :size 2}
                            {:url "mem:///123/456/aa"        :dir  true}
                            {:url "mem:///123/456/cc.txt"    :size 4}]

  (ls "mem:///"
      {:recurse true})  => [{:url "mem:///123"               :dir  true}
                            {:url "mem:///123/456.txt"       :size 1}
                            {:url "mem:///123/456"           :dir  true}
                            {:url "mem:///123/456/aa.txt"    :size 2}
                            {:url "mem:///123/456/aa"        :dir  true}
                            {:url "mem:///123/456/aa/bb.txt" :size 3}
                            {:url "mem:///123/456/cc.txt"    :size 4}]

  (mem/reset)

  ; check that files are returned in ascending order
  (spit (to (str "mem:///file-3.txt"))   "hello")
  (spit (to (str "mem:///file-2/2.txt")) "hello")
  (spit (to (str "mem:///file-1.txt"))   "hello")

  (->> (ls "mem:///")
       (map :url)) => (->> (ls "mem:///")
                           (map :url)
                           sort)

  (->> (ls "mem:///" {:recurse true})
       (map :url)) => (->> (ls "mem:///" {:recurse true})
                           (map :url)
                           sort)

  (mem/reset))

