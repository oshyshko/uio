(ns uio.codecs.gz
  (:require [uio.impl :refer [ext->is->is ext->os->os]])
  (:import (java.util.zip GZIPInputStream GZIPOutputStream)))

(defmethod ext->is->is :gz  [_] #(GZIPInputStream. %))
(defmethod ext->os->os :gz  [_] #(GZIPOutputStream. %))
