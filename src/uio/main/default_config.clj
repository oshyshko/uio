{
 ; For `kinit` authentication, use:
 ; "hdfs://" {}
 ;
 ; For keytab principal + path authentication, use:
 ; "hdfs://" {:principal "joe"
 ;            :keytab    "file:///path/to/principal.keytab"}
 ;
 ;
 "hdfs://" {}

 ; To include credentials from `~/.s3cfg`, add:
 ; "s3://" {}
 ;
 ; To use access/secret, add:
 ; "s3://"   {:access "access-key"
 ;            :secret "secret-key"}
 ;
 ;
 "s3://"   {}

 ; "sftp://" {:user                    "joe"
 ;            :pass                    "secret"                         ; optional (either :pass or :identity is required)
 ;            :identity                "<identity-value>"               ; optional, actual content
 ;            :identity-pass           "<identity-password-value>" }    ; optional, password for identity (if needed)
 ;            :known-hosts             "<ssh-rsa ...>"                  ; optional, actual content (ssh-rsa)
 ;            :skip-owner-group-lookup false                            ; optional, defaults to false
 ;
 ; NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa [-p <port>] <host>`
 ;       and copy the content (skip the line starting with a #).
 ;
 "sftp://" {:user        ""
            :pass        ""
            :known-hosts ""}

 ; NOTE: see also "Defining credentials for multiple fs and paths" at https://github.com/oshyshko/uio
 }
