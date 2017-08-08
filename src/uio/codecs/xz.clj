(ns uio.codecs.xz
  (:require [uio.impl :refer [ext->is->is ext->os->os]])
  (:import (org.apache.commons.compress.compressors CompressorStreamFactory)))

(defmethod ext->is->is :xz [_] #(.createCompressorInputStream  (CompressorStreamFactory.) CompressorStreamFactory/XZ %))
(defmethod ext->os->os :xz [_] #(.createCompressorOutputStream (CompressorStreamFactory.) CompressorStreamFactory/XZ %))
