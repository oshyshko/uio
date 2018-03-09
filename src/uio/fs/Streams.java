package uio.fs;

import clojure.lang.IFn;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Streams {
    public static class NullOutputStream extends OutputStream {
        public void write(int b) {
            // do nothing
        }

        public void write(byte[] b, int off, int len) {
            // do nothing
        }

        public String toString() {
            return "NullOutputStream";
        }
    }

    public static class StatsableInputStream extends FilterInputStream implements Statsable {
        private final AtomicLong count = new AtomicLong();

        public StatsableInputStream(InputStream in) {
            super(assertNotNull(in, "in"));
        }

        public int read() throws IOException {
            int b = in.read();
            if (b != -1)
                count.incrementAndGet();
            return b;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n != -1)
                count.addAndGet(n);
            return n;
        }

        public long getByteCount() {
            return count.get();
        }

        public String toString() {
            return "StatsableInputStream{byteCount=" + getByteCount() + ", in=" + in.getClass().getName() + "}";
        }
    }

    public static class StatsableOutputStream extends FilterOutputStream implements Statsable {
        private final AtomicLong count = new AtomicLong();

        public StatsableOutputStream(OutputStream out) {
            super(assertNotNull(out, "out"));
        }

        public void write(int b) throws IOException {
            out.write(b);
            count.incrementAndGet();
        }

        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            count.addAndGet(len);
        }

        public long getByteCount() {
            return count.get();
        }

        public String toString() {
            return "StatsableOutputStream{bytes=" + getByteCount() + ", out=" + out.getClass().getName() + "}";
        }
    }

    // closes itself upon reaching EOF or when garbage-collected
    public static class FinalizingInputStream extends FilterInputStream {
        public FinalizingInputStream(InputStream in) {
            super(in);
        }

        public int read() throws IOException {
            int b = super.read();
            if (b == -1) close();
            return b;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int n = super.read(b, off, len);
            if (n == -1) close();
            return n;
        }

        protected void finalize() throws Throwable {
            close();
        }

        public String toString() {
            return "FinalizingInputStream{in=" + in.getClass().getName() + "}";
        }

    }

    public static class ConcatInputStream extends InputStream {
        private final IFn url2is;
        private final LinkedList<String> urls;

        private InputStream is;

        public ConcatInputStream(IFn url2is, List<String> urls) {
            this.url2is = url2is;
            this.urls = new LinkedList<>(urls);

            if(!nextIs())
                throw new IllegalArgumentException("Argument 'urls' can't be empty");
        }

        // true  = there's next InputStream
        private boolean nextIs() {
            is = urls.isEmpty()
                    ? null
                    : new FinalizingInputStream((InputStream) url2is.invoke(urls.removeFirst()));
            return is != null;
        }

        public int read() throws IOException {
            if (is == null) return -1;
            for (;;) {
                int b = is.read();
                     if (b != -1)   return b;
                else if (!nextIs()) return -1;
                // else try reading from next stream
            }
        }

        public int read(byte[] b, int off, int len) throws IOException {
            if (is == null) return -1;
            for (;;) {
                int n = is.read(b, off, len);
                     if (n != -1)   return n;
                else if (!nextIs()) return -1;
                // else try reading from a new InputStream
            }
        }

        public int available() throws IOException {
            if (is == null)
                return 0;

            return is.available();
        }

        public void close() throws IOException {
            if (is != null)
                is.close();
            is = null;
            urls.clear();
        }

        public String toString() {
            return "ConcatInputStream{urls=" + urls + "}";
        }

    }

    public static class DigestibleInputStream extends InputStream {
        private final InputStream in;
        private final MessageDigest md;
        private byte[] digest;

        public DigestibleInputStream(String algorithm, InputStream in) throws NoSuchAlgorithmException {
            this.in = assertNotNull(in, "in");
            this.md = MessageDigest.getInstance(algorithm);
        }

        public int read() throws IOException {
            int b = in.read();
            if (b != -1)
                md.update((byte) b);
            return b;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n != -1)
                md.update(b, off, n);
            return n;
        }

        public void close() throws IOException {
            in.close();
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
                    ", in=" + in.getClass().getName() + "}";
        }
    }

    public static class DigestibleOutputStream extends OutputStream {
        private final OutputStream out;
        private final MessageDigest md;
        private byte[] digest;

        public DigestibleOutputStream(String algorithm, OutputStream out) throws NoSuchAlgorithmException {
            this.out = assertNotNull(out, "out");
            this.md = MessageDigest.getInstance(algorithm);
        }

        public void write(int b) throws IOException {
            out.write(b);
            md.update((byte) b);
        }

        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            md.update(b, off, len);
        }

        public void flush() throws IOException {
            out.flush();
        }

        public void close() throws IOException {
            out.close();
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
                    ", out=" + out.getClass().getName() + "}";
        }
    }

    public static class Finalizer implements AutoCloseable {
        private IFn f;

        public Finalizer(IFn f) {
            this.f = assertNotNull(f, "f");
        }

        public synchronized void close() {
            if (f == null)
                return;
            f.invoke();
            f = null;
        }

        protected synchronized void finalize() {
            close();
        }

        public String toString() {
            return "Finalizer{f=" + f + '}';
        }
    }

    public static class TakeNInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        public TakeNInputStream(long remaining, InputStream in) {
            this.in = in;
            this.remaining = remaining;
        }

        public int read() throws IOException {
            if (remaining > 0) {
                remaining--;
                return in.read();
            }
            return -1;
        }

        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining == 0)
                return -1;

            int n = in.read(b, off, Math.min((int) remaining, len));
            if (n >= 0)
                remaining -= n;

            return n;
        }

        public void close() throws IOException {
            in.close();
        }

        public String toString() {
            return "TakeNInputStream{in=" + in + ", remaining=" + remaining + '}';
        }
    }

    private static <T> T assertNotNull(T t, String arg) {
        if (t == null)
            throw new NullPointerException("Argument `" + arg + "` can't be null");
        return t;
    }
    
    public interface Statsable {
        long getByteCount();
    }
}
