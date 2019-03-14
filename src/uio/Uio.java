package uio;

import clojure.java.api.Clojure;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static clojure.java.api.Clojure.var;

// See `test/uio/Example.java`
public class Uio {
    static { var("clojure.core", "require").invoke(Clojure.read("uio.uio")); }

    private static final IFn FROM   = var("uio.uio/from");
    private static final IFn FROM_S = var("uio.uio/from*");
    private static final IFn TO     = var("uio.uio/to");
    private static final IFn TO_S   = var("uio.uio/to*");
    private static final IFn SIZE   = var("uio.uio/size");
    private static final IFn EXISTS = var("uio.uio/exists?");
    private static final IFn DELETE = var("uio.uio/delete");
    private static final IFn MKDIR  = var("uio.uio/mkdir");
    private static final IFn COPY   = var("uio.uio/copy");
    private static final IFn ATTRS  = var("uio.uio/attrs");
    private static final IFn LS     = var("uio.uio/ls");

    public static InputStream       from(String url)                           { return (InputStream)   FROM.invoke(url); }
    public static InputStream       from(String url, Map<String, Object> opts) { return (InputStream)   FROM.invoke(url, PersistentArrayMap.create(s2o_k2o(opts))); }
    public static InputStream       from(String url, long offset, long length) { return from(url, opts().offset(offset).length(length)); }

    public static InputStream decodeFrom(String url)                           { return (InputStream) FROM_S.invoke(url); }
    public static OutputStream  encodeTo(String url)                           { return (OutputStream)  TO_S.invoke(url); }
    public static OutputStream        to(String url)                           { return (OutputStream)    TO.invoke(url); }
    public static long              size(String url)                           { return (long)          SIZE.invoke(url); }
    public static boolean         exists(String url)                           { return (boolean)     EXISTS.invoke(url); }
    public static void            delete(String url)                           {                      DELETE.invoke(url); }
    public static void             mkdir(String url)                           {                       MKDIR.invoke(url); }
    public static void              copy(String fromUrl, String toUrl)         {                        COPY.invoke(fromUrl, toUrl); }
    public static void             attrs(String url)                           {                       ATTRS.invoke(url); }
    public static Iterable<Entry>     ls(String url)                           { return ls(url, opts()); }
    public static Iterable<Entry>     ls(String url, Map<String, Object> opts) { return ((List<Map<Keyword, Object>>) LS.invoke(url, PersistentArrayMap.create(s2o_k2o(opts))))
                                                                                        .stream()
                                                                                        .map(Uio::k2o_entry)::iterator; }
    private static Map<String, Object>  k2o_s2o(Map<Keyword, ?> k2o) { return k2o.entrySet().stream().collect(Collectors.toMap(kv -> kv.getKey().getName(),       Map.Entry::getValue));}
    private static Map<Keyword, Object> s2o_k2o(Map<String, ?>  s2o) { return s2o.entrySet().stream().collect(Collectors.toMap(kv -> Keyword.intern(kv.getKey()), Map.Entry::getValue));}

    private static Entry k2o_entry(Map<Keyword, Object> k2o) {
        Map<String, Object> s2o = k2o_s2o(k2o);

        String url     = (String) s2o.get("url");
        boolean isDir  = Boolean.TRUE.equals(s2o.get("dir"));
        Long size      = (Long) s2o.get("size");

        s2o.remove("url");
        s2o.remove("dir");
        s2o.remove("size");

        return new Entry(url,
                !isDir,
                isDir,
                size != null ? size : -1,
                Collections.unmodifiableMap(s2o));
    }

    public static class Opts extends HashMap<String, Object> {
        public static final Opts RECURSE = opts("recurse", true);

        public Opts add(String k, Object v) {
            put(k, v);
            return this;
        }
        public Opts add(Map<String, Object> m) {
            for (Entry<String, Object> kv : m.entrySet())
                put(kv.getKey(), kv.getValue());
            return this;
        }
        public Opts recurse(boolean v) { return add("recurse", v); }
        public Opts offset (long    v) { return add("offset",  v); }
        public Opts length (long    v) { return add("length",  v); }
    }

    public static Opts opts()                   { return new Opts(); }
    public static Opts opts(String k, Object v) { return opts().add(k, v); }

    public static class Entry {
        public final String url;
        public final boolean file;
        public final boolean dir;
        public final long size;
        public final Map<String, ?> extra;

        @Deprecated
        public Entry(String url, boolean file, boolean dir, long size, Map<String, ?> extra) {
            this.url = url;
            this.dir = dir;
            this.file = file;
            this.size = size;
            this.extra = extra;
        }

        public String           getUrl() { return url; }
        public boolean           isDir() { return dir; }
        public boolean          isFile() { return file; }
        public long            getSize() { return size; }
        @Deprecated
        public Map<String, ?> getExtra() { return extra; }

        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Entry entry = (Entry) o;

            if (dir != entry.dir) return false;
            if (file != entry.file) return false;
            if (size != entry.size) return false;
            if (!url.equals(entry.url)) return false;
            return extra.equals(entry.extra);
        }
        public int hashCode() {
            int result = url.hashCode();
            result = 31 * result + (dir ? 1 : 0);
            result = 31 * result + (file ? 1 : 0);
            result = 31 * result + (int) (size ^ (size >>> 32));
            result = 31 * result + extra.hashCode();
            return result;
        }
        public String toString() {
            return url +
                   (isFile() ? " " + size : "") +
                   (extra.isEmpty() ? "" : " " + extra.toString());
        }
    }
}
