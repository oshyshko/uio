package uio.fs;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class S3 {
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
