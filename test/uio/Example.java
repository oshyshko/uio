package uio;

import java.io.*;
import uio.Uio.*;

/**
 *  Examples:
 *   1. Reading and Writing
 *   2. Listing files
 *   3. Deleting
 *   4. Checking for existence
 *   5. Creating directories
 *   6. Copying files
 */
public class Example {
    public static void main(String[] args) throws IOException {
        // 1. Reading and Writing
        // ... give me an InputStream from ... HDFS, S3, SFTP, local FS, memory, HTTP, class loader resource
        try (InputStream is = Uio.from("hdfs:///path/to.txt")) {
            System.out.println("read: " + is.read());
        }
        // ^^^ prevent memory leaks and data corruption by using
        //     `try (Closeable c = ...) { ... }` pattern to automatically close resources.

        // ... give me an OutputStream to ... HDFS, S3, SFTP, local FS, memory,
        try (OutputStream os = Uio.to("s3://bucket/path/to/file.txt")) {
            os.write(42);
        }

        // ... give me an InputStream + decompress using GZIP (deduced from ".gz" extension)
        try (InputStream is = Uio.decodeFrom("hdfs:///path/to.txt.gz")) {
            System.out.println("read: " + is.read());
        }

        // ... give me an OutputStream + compress using GZIP (deduced from ".gz" extension)
        try (OutputStream os = Uio.encodeTo("s3://bucket/path/to/file.txt.gz")) {
            os.write(42);
        }

        // ... transformations can be chained: decompress using BZ2, ZX and then GZIP -- get the content of ".txt"
        try (InputStream is = Uio.decodeFrom("hdfs:///path/to.txt.gz.xz.bz2")) {
            System.out.println("read: " + is.read());
        }


        // 2. Listing files
        for (Entry e : Uio.ls("file:///"))
            System.out.println(e.getUrl() + (e.isFile() ? " " + e.getSize() : ""));
        // ^^^ returns Iterable<Uio.Entry>, where Entry is a JavaBean:
        //     class Entry {
        //       String  getUrl()
        //       boolean isDir()
        //       boolean isFile()
        //       long    getSize()
        //       ...
        //     }

        // ... calculate directory size (list files recursively)
        long size = 0;
        for (Entry e : Uio.ls("hdfs://dev/user/joe", Opts.RECURSE))
            if (e.isFile())
                size += e.getSize();
        System.out.println("Total size: " + size);


        // 3. Deleting
        Uio.delete("file:///path/to/file.txt");
        // ^^^ returns void, throws an exception if can't delete

        // 4. Checking for existence
        if (Uio.exists("file:///Users"))
            System.out.println("it does exist");


        // 5. Creating directories
        Uio.mkdir("file:///path/to/dir");
        // ^^^ returns void, throws an exception if can't create
        // ^^^ returns immediately for the file system doesn't have directories (e.g. s3 or mem)


        // 6. Copying files
        Uio.copy("hdfs:///path/to/file.txt.gz",
                 "s3://bucket/path/to/file.txt.gz");
        // ^^^ returns void, throws an exception if can't read source or write target file
        // ^^^ preserves the original content (does not attempt do decompress/compress files)
    }
}
