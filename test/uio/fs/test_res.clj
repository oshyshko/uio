(ns uio.fs.test-res
  (:require [midje.sweet :refer :all]
            [uio.fs.mem :refer :all]
            [uio.impl :refer :all]))

(facts "from"
  (slurp (from "res:///test/test.txt")) => "gdbg\n")

(facts "exists"
  (exists? "res:///") => true
  (exists? "res:///test") => true
  (exists? "res:///test/") => true
  (exists? "res:///test/te") => false
  (exists? "res:///test/test.txt") => true)

(facts "Listing"
  (fact "listing root succeeds"
    (ls "res:///") =not=> [])
  (fact "listing test dir gives correct results"
    (count (ls "res:///test/")) => 1
    (:url (first (ls "res:///test"))) => (has-suffix "test/test.txt")
    (:url (first (ls "res:///test/"))) => (has-suffix "test/test.txt"))
  (fact "listing test file returns just the same file"
    (count (ls "res:///test/test.txt")) => 1
    (:url (first (ls "res:///test/test.txt"))) => (has-suffix "test/test.txt")))
