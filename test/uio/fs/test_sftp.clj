(ns uio.fs.test-sftp
  (:require [uio.fs.sftp :refer :all]
            [midje.sweet :refer :all]))

(facts "private keys without line feeds work"
  (reformat-private-key-if-needed "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----")
  =>                              "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----"

  (reformat-private-key-if-needed "-----BEGIN RSA PRIVATE KEY----- 1 2 3 -----END RSA PRIVATE KEY-----")
  =>                              "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----")
