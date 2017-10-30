package uio.fs;

import clojure.lang.Counted;
import clojure.lang.IFn;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
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
            this.is = assertNotNull(is, "is");
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
            this.os = assertNotNull(os, "os");
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
            this.is = assertNotNull(is, "is");
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
            this.os = assertNotNull(os, "os");
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

    public static class Finalizer implements AutoCloseable {
        private IFn f;

        public Finalizer(IFn f) {
            this.f = assertNotNull(f, "f");
        }

        public synchronized void close() throws Exception {
            if (f == null)
                return;
            f.invoke();
            f = null;
        }

        protected synchronized void finalize() throws Throwable {
            close();
        }

        public String toString() {
            return "Finalizer{f=" + f + '}';
        }
    }

    public static class TakeNInputStream extends InputStream {
        private final InputStream is;
        private long remaining;

        public TakeNInputStream(long remaining, InputStream is) {
            this.is = is;
            this.remaining = remaining;
        }

        public int read() throws IOException {
            if (remaining > 0) {
                remaining--;
                return is.read();
            }
            return -1;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining == 0)
                return -1;

            int n = is.read(b, off, Math.min((int) remaining, len));
            if (n >= 0)
                remaining -= n;

            return n;
        }

        public void close() throws IOException {
            is.close();
        }

        public String toString() {
            return "TakeNInputStream{is=" + is + ", remaining=" + remaining + '}';
        }
    }

    private static <T> T assertNotNull(T t, String arg) {
        if (t == null)
            throw new NullPointerException("Argument `" + arg + "` can't be null");
        return t;
    }
}
