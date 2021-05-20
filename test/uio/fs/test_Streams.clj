(ns uio.fs.test-Streams
  (:require [midje.sweet :refer :all]
            [uio.impl :as impl])
  (:import [uio.fs Streams$TakeNInputStream Streams$FinalizingInputStream Streams$ConcatInputStream]
           [java.io ByteArrayInputStream]))

(facts "Streams$TakeNInputStream works"
  (->> (impl/bytes->is (.getBytes "hello world"))
       (Streams$TakeNInputStream. 5)
       slurp)
  => "hello")

(defn ->logging-is [*state content]
  (reset! *state :open)
  (proxy [ByteArrayInputStream]
         [(.getBytes content)]
    (close [] (reset! *state :closed))))

(facts "FinalizingInputStream works"
  (facts "closing twice doesn't fail"
    (let [*state (atom false)
          is     (Streams$FinalizingInputStream. (->logging-is *state "abc"))]

      @*state => :open
      (.close is)
      @*state => :closed
      (.close is)
      @*state => :closed))

  (fact "reaching EOF of stream"
    (let [*state (atom false)
          is     (Streams$FinalizingInputStream. (->logging-is *state "abc"))]

      @*state => :open
      (.read is) => (int \a)
      (.read is) => (int \b)
      (.read is) => (int \c)
      (.read is) => -1
      @*state => :closed
      (.close is)
      @*state => :closed))

  (fact "GC closes the stream"
    (let [*state (atom false)]
      (Streams$FinalizingInputStream. (->logging-is *state "abc"))

      @*state => :open
      (System/gc)
      (Thread/sleep 1000)
      (System/gc)
      @*state => :closed)))

(facts "ConcatInputStream works"
  (fact "reads to the end"
    (let [in2*states  {"0" (atom nil)
                       "1" (atom nil)
                       "2" (atom nil)}
          list-states #(map deref (vals in2*states))
          is          (Streams$ConcatInputStream. #(->logging-is (in2*states %) (str % "ab"))
                                                  (keys in2*states))]

      (list-states) => [:open nil nil]

      (.available is) => 3 (.read is) => (int \0)
      (.available is) => 2 (.read is) => (int \a)
      (.available is) => 1 (.read is) => (int \b) (list-states) => [:open nil nil]

      (.available is) => 0 (.read is) => (int \1) (list-states) => [:closed :open nil]
      (.available is) => 2 (.read is) => (int \a)
      (.available is) => 1 (.read is) => (int \b) (list-states) => [:closed :open nil]

      (.available is) => 0 (.read is) => (int \2) (list-states) => [:closed :closed :open]
      (.available is) => 2 (.read is) => (int \a)
      (.available is) => 1 (.read is) => (int \b) (list-states) => [:closed :closed :open]

      (.available is) => 0 (.read is) => -1

      (list-states) => [:closed :closed :closed]))

  (fact "closing in the middle closes current stream and doesn't open the rest"
    (let [in2*states  {"0" (atom nil)
                       "1" (atom nil)}
          list-states #(map deref (vals in2*states))
          is          (Streams$ConcatInputStream. #(->logging-is (in2*states %) (str % "ab"))
                                                  (keys in2*states))]

      (list-states) => [:open nil]

      (.available is) => 3 (.read is) => (int \0)
      (.available is) => 2 (.read is) => (int \a)
      (.available is) => 1 (.read is) => (int \b)
      (.available is) => 0

      (list-states) => [:open nil]
      (.close is)
      (list-states) => [:closed nil]

      (.read is) => -1
      )))
