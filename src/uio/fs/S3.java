package uio.fs;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class S3 {
    public static class S3OutputStream extends OutputStream {
        // https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
        // Maximum object size	                5 TB
        // Maximum number of parts per upload	10,000
        // Part numbers	                        1 to 10,000 (inclusive)
        // Part size	                        5 MB to 5 GB, last part can be < 5 MB
        //
        // 549,755,814 bytes per part -- enough to cover a 5TB file with 10000 parts
        private static final int PART_SIZE = (int) (((5L * 1024 * 1024 * 1024 * 1024) / 10000) + 1);

        private final AmazonS3Client c;
        private final InitiateMultipartUploadResult init;

        private final List<PartETag> tags = new ArrayList<>();

        private final MessageDigest inDigest = MessageDigest.getInstance("MD5");
        private final MessageDigest outDigest = MessageDigest.getInstance("MD5");
        private final MessageDigest partDigest = MessageDigest.getInstance("MD5");

        private final File partTempFile;
        private Streams.StatsableOutputStream partOutputStream;
        private int partIndex;

        private boolean closed;

        public S3OutputStream(AmazonS3Client c, String bucket, String key, CannedAccessControlList cannedAclOrNull) throws NoSuchAlgorithmException, IOException {
            this.c = c;

            init = c.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)
                    .withCannedACL(cannedAclOrNull)); // setting only here, not setting in UploadPartRequest

            partTempFile = Files.createTempFile("uio-s3-part-", ".tmp").toFile();
            partOutputStream = new Streams.StatsableOutputStream(new FileOutputStream(partTempFile));
        }

        public void write(int b) throws IOException {
            write(new byte[]{(byte) b});
        }

        public void write(byte[] bs, int offset, int length) throws IOException {
            assertOpen();
            inDigest.update(bs, offset, length);

            while (length != 0) {
                // flush buffer if full
                if (PART_SIZE == partOutputStream.getByteCount())
                    _flush(false);

                int bytesToCopy = Math.min(PART_SIZE - (int) partOutputStream.getByteCount(), length);

                // append to buffer
                partOutputStream.write(bs, offset, bytesToCopy);

                partDigest.update(bs, offset, bytesToCopy);
                outDigest.update(bs, offset, bytesToCopy);

                offset += bytesToCopy;
                length -= bytesToCopy;
            }
        }

        private void assertOpen() throws IOException {
            if (closed)
                throw new IOException("Can't write to closed stream");
        }

        private void _flush(boolean isLastPart) throws IOException {
            partOutputStream.close();

            try {
                UploadPartRequest upr = new UploadPartRequest()
                        .withBucketName(init.getBucketName())
                        .withKey(init.getKey())
                        .withUploadId(init.getUploadId())
                        .withPartNumber(partIndex + 1)
                        .withFile(partTempFile)
                        .withPartSize(partOutputStream.getByteCount())
                        .withLastPart(isLastPart);

                PartETag remotePartEtag = c.uploadPart(upr).getPartETag();
                tags.add(remotePartEtag);


                partDigest.reset();
                partOutputStream = new Streams.StatsableOutputStream(new FileOutputStream(partTempFile));
                partIndex++;
            } catch (Exception e) {
                abort();
                throw e;
            }
        }

        public void close() throws IOException {
            if (closed)
                return;

            _flush(true);
            try {
                String read = hex(inDigest.digest());
                String written = hex(outDigest.digest());

                if (!read.equals(written))
                    throw new RuntimeException("Local MD5s don't match:\n" +
                            " - read   : " + read + "\n" +
                            " - written: " + written);

                c.completeMultipartUpload(new CompleteMultipartUploadRequest(init.getBucketName(), init.getKey(), init.getUploadId(), tags));

                partDigest.reset();
                for (PartETag tag : tags) {
                    partDigest.update(unhex(tag.getETag()));
                }

            } catch (Exception e) {
                abort(); // TODO delete remote file if exception happened after `c.completeMultipartUpload(...)`
                throw e;
            }
            closed = true;
            partTempFile.delete();
        }

        private static String hex(byte[] bs) {
            return DatatypeConverter.printHexBinary(bs).toLowerCase();
        }

        private static byte[] unhex(String s) {
            return DatatypeConverter.parseHexBinary(s);
        }

        private void abort() {
            c.abortMultipartUpload(new AbortMultipartUploadRequest(init.getBucketName(), init.getKey(), init.getUploadId()));
        }

        public String toString() {
            return "S3OutputStream{bucket='" + init.getBucketName() + '\'' + ", key='" + init.getKey() + '\'' + '}';
        }
    }
}
