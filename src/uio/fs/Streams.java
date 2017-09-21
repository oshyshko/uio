package uio.fs;

import clojure.lang.Counted;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Streams {
    public static class NullOutputStream extends OutputStream {
        public void write(int b) throws IOException {
            // do nothing
        }

        public void write(byte[] b, int off, int len) throws IOException {
            // do nothing
        }

        public String toString() {
            return "NullOutputStream";
        }
    }

    public static class CountableInputStream extends InputStream implements Counted {
        private final InputStream is;
        private final AtomicInteger count = new AtomicInteger();

        public CountableInputStream(InputStream is) {
            if (is == null)
                throw new IllegalArgumentException("Argument `is` can't be null");
            this.is = is;
        }

        public int read() throws IOException {
            int b = is.read();
            if (b != -1)
                count.incrementAndGet();
            return b;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int n = is.read(b, off, len);
            if (n != -1)
                count.addAndGet(n);
            return n;
        }

        public int count() {
            return count.get();
        }

        public String toString() {
            return "CountableInputStream{count=" + count() + ", is=" + is.getClass().getName() + "}";
        }
    }

    public static class CountableOutputStream extends OutputStream implements Counted {
        private final OutputStream os;
        private final AtomicInteger count = new AtomicInteger();

        public CountableOutputStream(OutputStream os) {
            if (os == null)
                throw new NullPointerException("Argument `os` can't be null");

            this.os = os;
        }

        public void write(int b) throws IOException {
            os.write(b);
            count.incrementAndGet();
        }

        public void write(byte[] b, int off, int len) throws IOException {
            os.write(b, off, len);
            count.addAndGet(len);
        }

        public void flush() throws IOException {
            os.flush();
        }

        public void close() throws IOException {
            os.close();
        }

        public int count() {
            return count.get();
        }

        public String toString() {
            return "CountableOutputStream{count=" + count() + ", os=" + os.getClass().getName() + "}";
        }
    }

    public static class DigestibleInputStream extends InputStream {
        private final InputStream is;
        private final MessageDigest md;
        private byte[] digest;

        public DigestibleInputStream(String algorithm, InputStream is) throws NoSuchAlgorithmException {
            if (is == null)
                throw new NullPointerException("Argument `is` can't be null");

            this.is = is;
            this.md = MessageDigest.getInstance(algorithm);
        }

        public int read() throws IOException {
            int b = is.read();
            if (b != -1)
                md.update((byte) b);
            return b;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int n = is.read(b, off, len);
            if (n != -1)
                md.update(b, off, n);
            return n;
        }

        public void close() throws IOException {
            is.close();
            if (digest == null)
                digest = md.digest();
        }

        public byte[] closeAndDigest() throws IOException {
            close();
            return Arrays.copyOf(digest, digest.length);
        }

        public String toString() {
            return "DigestibleInputStream{algorithm=" + md.getAlgorithm() +
                    ", digest=" + (digest == null ? "null" : DatatypeConverter.printHexBinary(digest)) +
                    ", is=" + is.getClass().getName() + "}";
        }
    }

    public static class DigestibleOutputStream extends OutputStream {
        private final OutputStream os;
        private final MessageDigest md;
        private byte[] digest;

        public DigestibleOutputStream(String algorithm, OutputStream os) throws NoSuchAlgorithmException {
            if (os == null)
                throw new NullPointerException("Argument `os` can't be null");

            this.os = os;
            this.md = MessageDigest.getInstance(algorithm);
        }

        public void write(int b) throws IOException {
            os.write(b);
            md.update((byte) b);
        }

        public void write(byte[] b, int off, int len) throws IOException {
            os.write(b, off, len);
            md.update(b, off, len);
        }

        public void flush() throws IOException {
            os.flush();
        }

        public void close() throws IOException {
            os.close();
            if (digest == null)
                digest = md.digest();
        }

        public byte[] closeAndDigest() throws IOException {
            close();
            return Arrays.copyOf(digest, digest.length);
        }

        public String toString() {
            return "DigestibleOutputStream{algorithm=" + md.getAlgorithm() +
                    ", digest=" + (digest == null ? "null" : DatatypeConverter.printHexBinary(digest)) +
                    ", os=" + os.getClass().getName() + "}";
        }
    }

    public static class S3OutputStream extends OutputStream {
        private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5MB -- required minimum by S3 API

        private final AmazonS3Client c;
        private final String bucket;
        private final String key;

        private final List<PartETag> tags = new ArrayList<>();

        private final InitiateMultipartUploadResult res;

        private final byte[] buffer = new byte[BUFFER_SIZE];
        private final MessageDigest inDigest = MessageDigest.getInstance("MD5");
        private final MessageDigest outDigest = MessageDigest.getInstance("MD5");

        private final MessageDigest localPartDigest = MessageDigest.getInstance("MD5");

        private int bufferOffset;
        private int partIndex;

        public S3OutputStream(AmazonS3Client c, String bucket, String key) throws NoSuchAlgorithmException {
            this.c = c;
            this.bucket = bucket;
            this.key = key;

            res = c.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key));
        }

        public void write(int b) throws IOException {
            write(new byte[]{(byte) b});
        }

        public void write(byte[] bs, int offset, int length) throws IOException {
            inDigest.update(bs, offset, length);

            while (length != 0) {
                // flush buffer if full
                if (bufferOffset == BUFFER_SIZE) {
                    _flush(false);
                    bufferOffset = 0;
                }

                int bytesToCopy = Math.min(BUFFER_SIZE - bufferOffset, length);

                // move to buffer
                System.arraycopy(
                        bs, offset,
                        buffer, bufferOffset,
                        bytesToCopy);

                bufferOffset += bytesToCopy;

                offset += bytesToCopy;
                length -= bytesToCopy;
            }
        }

        private void _flush(boolean isLastPart) throws IOException {
            outDigest.update(buffer, 0, bufferOffset);

            localPartDigest.reset();
            localPartDigest.update(buffer, 0, bufferOffset);

            String localPartEtag = hex(localPartDigest.digest());
            try {
                UploadPartRequest upr = new UploadPartRequest()
                        .withBucketName(bucket)
                        .withKey(key)
                        .withUploadId(res.getUploadId())
                        .withPartNumber(partIndex + 1)
                        .withInputStream(new ByteArrayInputStream(buffer, 0, bufferOffset))
                        .withPartSize(bufferOffset)
                        .withLastPart(isLastPart);

                PartETag remotePartEtag = c.uploadPart(upr).getPartETag();
                tags.add(remotePartEtag);

                if (!remotePartEtag.getETag().equals(localPartEtag)) {
                    throw new RuntimeException("Part ETags don't match:\n" +
                            " - local : " + localPartEtag + "\n" +
                            " - remote: " + remotePartEtag.getETag());
                }

                partIndex++;
            } catch (Exception e) {
                abort();
                throw e;
            }
        }

        public void close() throws IOException {
            _flush(true);
            try {
                String read = hex(inDigest.digest());
                String written = hex(outDigest.digest());

                if (!read.equals(written))
                    throw new RuntimeException("Local MD5s don't match:\n" +
                            " - read   : " + read + "\n" +
                            " - written: " + written);

                String remoteEtag = c.completeMultipartUpload(new CompleteMultipartUploadRequest(bucket, key, res.getUploadId(), tags)).getETag();

                localPartDigest.reset();
                for (PartETag tag : tags) {
                    localPartDigest.update(unhex(tag.getETag()));
                }
                String localEtag = hex(localPartDigest.digest()) + "-" + partIndex;

                if (!localEtag.equals(remoteEtag))
                    throw new RuntimeException("Etags don't match:\n" +
                            " - local : " + localEtag + "\n" +
                            " - remote: " + remoteEtag);
            } catch (Exception e) {
                abort(); // TODO delete remote file if exception happened after `c.completeMultipartUpload(...)`
                throw e;
            }
        }

        private static String hex(byte[] bs) {
            return DatatypeConverter.printHexBinary(bs).toLowerCase();
        }

        private static byte[] unhex(String s) {
            return DatatypeConverter.parseHexBinary(s);
        }

        private void abort() {
            c.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, res.getUploadId()));
        }

        public String toString() {
            return "S3OutputStream{bucket='" + bucket + '\'' + ", key='" + key + '\'' + '}';
        }
    }
}
