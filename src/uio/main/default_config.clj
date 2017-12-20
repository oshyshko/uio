{
 ; Default settings per schema
 ;
 ; NOTE: for `kinit` authentication, leave empty settings {}
 ; NOTE: for keytab principal + path authentication, use these settings:
 ;
 ; "hdfs://" {:principal "joe"
 ;            :keytab    "file:///path/to/principal.keytab"}
 ;
 ; NOTE: to use `kinit` for authentication, add this entry:
 ;
 ; "hdfs://" {}
 ;
 "hdfs://" {}

 ; "s3://"   {:access "access-key"
 ;            :secret "secret-key"}
 ;
 ; NOTE: to use credentials from `~/.s3cfg`, add this entry:
 ;
 ; "s3://" {}
 ;
 "s3://"   {}

 ; "sftp://" {:user          "joe"
 ;            :pass          "secret"                         ; optional
 ;            :known-hosts   "<ssh-rsa ...>"                  ; actual content (ssh-rsa)
 ;            :identity      "<identity-value>"               ; optional, actual content
 ;            :identity-pass "<identity-password-value>" }    ; optional, password for identity (if needed)
 ;
 ; NOTE: to get a value for known hosts, use `$ ssh-keyscan -t ssh-rsa [-p <port>] <host>`
 ;       and copy the content (skip the line starting with a #).
 ;
 "sftp://" {:user        ""
            :known-hosts ""
            :pass        ""}

 ; NOTE: see also "Defining credentials for multiple fs and paths" at https://github.com/oshyshko/uio
 }
