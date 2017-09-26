(defproject uio/uio "1.1-SNAPSHOT"
  :description "uio is a Clojure/Java library for accessing HDFS, S3, SFTP and other file systems via a single API."

  :repositories {"cloudera" "https://repository.cloudera.com/content/groups/cdh-releases-rcs"}

  :deploy-repositories [["clojars" {:url           "https://clojars.org/repo/"
                                    :sign-releases false}]]

  :dependencies [[org.clojure/clojure "1.8.0"]

                 [com.amazonaws/aws-java-sdk-s3 "1.11.201"] ; s3
                 [org.apache.httpcomponents/httpclient "4.5.3"] ; (needed by `aws-java-sdk-s3`)

                 [com.jcraft/jsch "0.1.54"]                 ; sftp
                 [com.jcraft/jzlib "1.1.3"]                 ; (needed by `jsch`)

                 [org.apache.hadoop/hadoop-common "2.8.0"   ; hdfs
                  :exclusions [org.apache.httpcomponents/httpcore]] ; conflicts with `aws-java-sdk-s3`
                 [org.apache.hadoop/hadoop-hdfs "2.8.0"]

                 [org.apache.commons/commons-compress "1.12"] ; bzip2, xz
                 [org.tukaani/xz "1.6"]]                    ; xz (needed by `commons-compress`)

  ; TODO not working. Find a way to exclude .java files from JARs, but have 1 source dir for Java/Clojure
  ;:jar-exclusions [#".*\.java"]

  :java-source-paths ["src"]
  :javac-options ["-source" "1.8"
                  "-target" "1.8"]

  :profiles {:dev {:dependencies [[midje "1.8.3"]]
                   :plugins      [[lein-midje "3.2.1"]]}}

  :aot [uio.main]
  :main uio.main

  :uberjar-name "uio.jar"
  :bin {:name "uio"}

  :plugins [[lein-bin "0.3.4"]]

  ; A trick to prevent IntelliJ from resetting compiler/module version to "1.5"
  :pom-plugins [[org.apache.maven.plugins/maven-compiler-plugin "3.6.1"
                 [:configuration ([:source "1.8"]
                                   [:target "1.8"])]]])

