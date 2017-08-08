(ns uio.codecs.bz2
  (:require [uio.impl :refer [ext->is->is ext->os->os]])
  (:import (org.apache.commons.compress.compressors CompressorStreamFactory)))

(defmethod ext->is->is :bz2 [_] #(.createCompressorInputStream  (CompressorStreamFactory.) CompressorStreamFactory/BZIP2 %))
(defmethod ext->os->os :bz2 [_] #(.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/BZIP2 %))
