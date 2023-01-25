(defproject uio/uio "1.2.4"
  :description "uio is a Clojure library and a command line tool for accessing HDFS, S3, SFTP and other file systems."

  :repositories {"cloudera"  "https://repository.cloudera.com/content/groups/cdh-releases-rcs"
                 "foursquare"  {:url      "https://foursquaredev.jfrog.io/foursquaredev/fsnexus"
                                :username :env/MVN_USERNAME :password :env/MVN_PASSWORD}}

  :deploy-repositories {"snapshots" {:id          "foursquare"
                                     :url         "https://foursquaredev.jfrog.io/foursquaredev/fsfactual-snapshots-local"
                                     :username :env/MVN_USERNAME :password :env/MVN_PASSWORD
                                     :sign-releases false}
                        "releases"  {:id          "foursquare"
                                     :url         "https://foursquaredev.jfrog.io/foursquaredev/fsfactual-releases-local"
                                     :username :env/MVN_USERNAME :password :env/MVN_PASSWORD
                                     :sign-releases false}}

  :dependencies [[org.clojure/clojure "1.9.0"]

                 [com.amazonaws/aws-java-sdk-s3 "1.12.300"] ; s3
                 [com.amazonaws/aws-java-sdk-sts "1.12.300"] ; s3 with roles
                 [org.apache.httpcomponents/httpclient "4.5.13"] ; (needed by `aws-java-sdk-s3`)

                 [com.jcraft/jsch "0.1.55"]                 ; sftp
                 [com.jcraft/jzlib "1.1.3"]                 ; (needed by `jsch`)

                 [org.apache.hadoop/hadoop-common "2.8.1"   ; hdfs (API) note: 3.1.1 is available, but it can't find HDFS impl
                  :exclusions [org.apache.httpcomponents/httpcore]] ; conflicts with `aws-java-sdk-s3`
                 [org.apache.hadoop/hadoop-hdfs "2.8.1"]    ; (actual implementation)

                 [org.apache.commons/commons-compress "1.18"] ; bzip2, xz
                 [org.tukaani/xz "1.8"]                     ; xz (needed by `commons-compress`)

                 [org.clojure/tools.cli "0.4.1"]]           ; main

  ; TODO not working. Find a way to exclude .java files from JARs, but have 1 source dir for Java/Clojure
  ;:jar-exclusions [#".*\.java"]

  :java-source-paths ["src"]
  :javac-options ["-source" "1.8"
                  "-target" "1.8"
                  "-Xlint:deprecation"
                  "-Xlint:unchecked"]
  :resource-paths ["resources"]

  :profiles {:dev {:dependencies [[midje "1.9.2"]]
                   :plugins      [[lein-midje "3.2.1"]]}}

  :aot [uio.main.main]
  :main uio.main.main

  :uberjar-name "uio.jar"
  :bin {:name "uio"}

  :plugins [[lein-bin "0.3.4"]]

  ; A trick to prevent IntelliJ from resetting compiler/module version to "1.5"
  :pom-plugins [[org.apache.maven.plugins/maven-compiler-plugin "3.6.1"
                 [:configuration ([:source "1.8"]
                                  [:target "1.8"])]]])
