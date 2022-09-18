(defproject uio/uio "1.2-SNAPSHOT"
  :description "uio is a Clojure library and a command line tool for accessing HDFS, S3, SFTP and other file systems."

  :deploy-repositories [["clojars" {:url           "https://clojars.org/repo/"
                                    :sign-releases false}]]

  :dependencies [[org.clojure/clojure "1.11.1"]

                 [com.amazonaws/aws-java-sdk-s3 "1.12.300"] ; s3
                 [com.amazonaws/aws-java-sdk-sts "1.12.300"] ; s3 with roles
                 [org.apache.httpcomponents/httpclient "4.5.13"] ; (needed by `aws-java-sdk-s3`)

                 [com.jcraft/jsch "0.1.55"]                 ; sftp
                 [com.jcraft/jzlib "1.1.3"]                 ; (needed by `jsch`)

                 [org.apache.hadoop/hadoop-common "3.3.2"]
                 [org.apache.hadoop/hadoop-hdfs "3.3.2"]

                 [org.apache.commons/commons-compress "1.21"] ; bzip2, xz
                 [org.tukaani/xz "1.9"]                     ; xz (needed by `commons-compress`)

                 [org.clojure/tools.cli "1.0.206"]]         ; main

  :jar-exclusions [#".*\.java"]
  :java-source-paths ["src"]
  :javac-options ["-source" "1.8"
                  "-target" "1.8"
                  "-Xlint:deprecation"
                  "-Xlint:unchecked"]

  :profiles {:dev {:dependencies [[midje "1.10.5"]]
                   :plugins      [[lein-midje "3.2.1"]]}}

  :aot [uio.main.main]
  :main uio.main.main

  :uberjar-name "uio.jar"
  :bin {:name "uio"
        :bootclasspath true}

  :plugins [[lein-bin "0.3.5"]
            [lein-pprint "1.3.2"]]
  )
