(ns uio.fs.test-file
  (:require [uio.impl :refer :all]
            [midje.sweet :refer :all]))

(facts "Listing root doesn't throw exceptions"
  (ls "file:///") =not=> [])