(ns uio.fs.test-sftp
  (:require [uio.fs.sftp :refer :all]
            [midje.sweet :refer :all]
            [clojure.string :as str]))

(facts "private keys without line feeds work"
  (reformat-private-key-if-needed "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----")
  =>                              "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----"

  (reformat-private-key-if-needed "-----BEGIN RSA PRIVATE KEY----- 1 2 3 -----END RSA PRIVATE KEY-----")
  =>                              "-----BEGIN RSA PRIVATE KEY-----\n123\n-----END RSA PRIVATE KEY-----")

(facts "passwd->uid+gid->name"
  (passwd->id->name
    (str/join "\n" ["root:x:0:0:root:/root:/bin/bash"
                    "sync:x:4:65534:sync:/bin:/bin/sync"
                    "games:x:5:60:games:/usr/games:/usr/sbin/nologin"])) => {0 "root"
                                                                             4 "sync"
                                                                             5 "games"}
  (passwd->id->name
    (str/join "\n" ["root:x:0:"
                    "daemon:x:1:"
                    "bin:x:2:"])) => {0 "root"
                                      1 "daemon"
                                      2 "bin"})
