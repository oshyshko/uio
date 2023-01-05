(ns uio.main.test-main
  (:require [uio.main.main :refer :all]
            [midje.sweet :refer :all]))

(facts "size->human-size works"
  (size->human-size          0) =>   "0B"
  (size->human-size          1) =>   "1B"
  (size->human-size        125) => "125B"
  (size->human-size        999) => "999B"
  (size->human-size       1000) =>   (format "%.1fK" (float 1)) ; 1.0K <-- in case if default locale on build machine is different
  (size->human-size       1023) =>   (format "%.1fK" (float 1)) ; 1.0K
  (size->human-size       1024) =>   "1K"
  (size->human-size       1025) =>   (format "%.1fK" (float 1)) ; 1.0K
  (size->human-size       2048) =>   "2K"
  (size->human-size       8192) =>   "8K"
  (size->human-size      16384) =>  "16K"
  (size->human-size      32768) =>  "32K"
  (size->human-size      65536) =>  "64K"
  (size->human-size     131072) => "128K"
  (size->human-size     262144) => "256K"
  (size->human-size     524288) => "512K"
  (size->human-size    1048575) => (format "%.1fM" (float 1)) ; 1.0M
  (size->human-size    1048576) =>   "1M"
  (size->human-size    2097152) =>   "2M"
  (size->human-size    4194304) =>   "4M"
  (size->human-size 1073741823) => (format "%.1fG" (float 1)) ; 1.0G
  (size->human-size 1073741824) =>   "1G"
  (size->human-size 1073741825) => (format "%.1fG" (float 1)) ; 1.0G

  (size->human-size 9060276.4)  => (format "%.1fM" (float 8.6))) ; 8.6M