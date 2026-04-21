(ns toolkit.zorder-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.frng :as frng]
            [toolkit.zorder :as z]))

(deftest litmax-bigmin-2d-simple
  (is (= [221 298] (z/litmax-bigmin-2d 123 456))))

(deftest litmax-bigmin-2d-equal-inputs
  (is (= [3 3] (z/litmax-bigmin-2d 3 3))))

(deftest split-bbox-2d-basic
  (is (= [3 3 6 7 9 9 11 15 18 18 24 24 26 26 33 33 36 37 48 48]
         (z/split-bbox-2d 3 48))))

(deftest range-contained-in-bbox-2d-basic
  ;; (2, 3) in 2D covers coordinates (0,1) and (1,1) — a 1x2 box containing exactly 2 codes.
  (is (#'z/range-contained-in-bbox-2d? 2 3)))

(deftest well-formed-bbox-throws
  (is (thrown? IllegalArgumentException (z/split-bbox-2d 4 3))))

(deftest encode-2-decode-2-roundtrip
  (doseq [x (range 256) y (range 256)]
    (let [code (z/encode-2 x y)]
      (is (= x (z/decode-2-x code)))
      (is (= y (z/decode-2-y code))))))

(deftest encode-3-decode-3-roundtrip
  (doseq [x (range 64) y (range 64) z (range 64)]
    (let [code (z/encode-3 x y z)]
      (is (= x (z/decode-3-x code)))
      (is (= y (z/decode-3-y code)))
      (is (= z (z/decode-3-z code))))))

(defn- in-rect-2d? [^long z ^long lo ^long hi]
  (let [x (z/decode-2-x z) y (z/decode-2-y z)]
    (and (>= x (z/decode-2-x lo)) (<= x (z/decode-2-x hi))
         (>= y (z/decode-2-y lo)) (<= y (z/decode-2-y hi)))))

(defn- in-rect-3d? [^long z ^long lo ^long hi]
  (and (>= (z/decode-3-x z) (z/decode-3-x lo)) (<= (z/decode-3-x z) (z/decode-3-x hi))
       (>= (z/decode-3-y z) (z/decode-3-y lo)) (<= (z/decode-3-y z) (z/decode-3-y hi))
       (>= (z/decode-3-z z) (z/decode-3-z lo)) (<= (z/decode-3-z z) (z/decode-3-z hi))))

(defn- brute-force-bigmin-2d [^long lo ^long hi ^long div]
  (loop [z (inc div)]
    (cond
      (> z hi) nil
      (in-rect-2d? z lo hi) z
      :else (recur (inc z)))))

(defn- brute-force-litmax-2d [^long lo ^long hi ^long div]
  (loop [z (dec div)]
    (cond
      (< z lo) nil
      (in-rect-2d? z lo hi) z
      :else (recur (dec z)))))

(defn- brute-force-bigmin-3d [^long lo ^long hi ^long div]
  (loop [z (inc div)]
    (cond
      (> z hi) nil
      (in-rect-3d? z lo hi) z
      :else (recur (inc z)))))

(defn- brute-force-litmax-3d [^long lo ^long hi ^long div]
  (loop [z (dec div)]
    (cond
      (< z lo) nil
      (in-rect-3d? z lo hi) z
      :else (recur (dec z)))))

(defn- run-frng-sim!
  "Run an frng simulation via `frng/search` under clojure.test. On failure,
   surface the `{:size :seed}` replay recipe in the assertion message."
  [test-fn opts]
  (let [result (frng/search test-fn opts)]
    (is (= :pass result)
        (when (map? result)
          (let [{:keys [size seed ^Throwable ex]} (:fail result)]
            (str "simulation failed at size=" size " seed=" seed
                 " — replay via (frng/run-once <sim> {:size " size
                 " :seed " seed "}) — cause: " (.getMessage ex)))))))

(defn- bigmin-litmax-2d-sim [frng]
  (let [coord-max 63]
    (loop []
      (let [x0 (frng/int-inclusive frng coord-max)
            y0 (frng/int-inclusive frng coord-max)
            x1 (frng/range-inclusive frng x0 coord-max)
            y1 (frng/range-inclusive frng y0 coord-max)
            lo (z/encode-2 x0 y0)
            hi (z/encode-2 x1 y1)]
        (when (> hi (inc lo))
          (let [div (frng/range-inclusive frng (inc lo) (dec hi))]
            (when-not (in-rect-2d? div lo hi)
              (let [got  (z/bigmin lo hi div 2)
                    want (brute-force-bigmin-2d lo hi div)]
                (assert (= want got)
                        (format "BigMin(%d, %d, %d, 2) = %d, want %d" lo hi div got want)))
              (let [got  (z/litmax lo hi div 2)
                    want (brute-force-litmax-2d lo hi div)]
                (assert (= want got)
                        (format "LitMax(%d, %d, %d, 2) = %d, want %d" lo hi div got want))))))
        (recur)))))

(defn- bigmin-litmax-3d-sim [frng]
  ;; Keep the per-axis range small enough that the brute-force oracle
  ;; stays cheap — Z-codes are 3*bits wide, so 0..15 already gives codes up to 4095.
  (let [coord-max 15]
    (loop []
      (let [x0 (frng/int-inclusive frng coord-max)
            y0 (frng/int-inclusive frng coord-max)
            z0 (frng/int-inclusive frng coord-max)
            x1 (frng/range-inclusive frng x0 coord-max)
            y1 (frng/range-inclusive frng y0 coord-max)
            z1 (frng/range-inclusive frng z0 coord-max)
            lo (z/encode-3 x0 y0 z0)
            hi (z/encode-3 x1 y1 z1)]
        (when (> hi (inc lo))
          (let [div (frng/range-inclusive frng (inc lo) (dec hi))]
            (when-not (in-rect-3d? div lo hi)
              (let [got  (z/bigmin lo hi div 3)
                    want (brute-force-bigmin-3d lo hi div)]
                (assert (= want got)
                        (format "BigMin(%d, %d, %d, 3) = %d, want %d" lo hi div got want)))
              (let [got  (z/litmax lo hi div 3)
                    want (brute-force-litmax-3d lo hi div)]
                (assert (= want got)
                        (format "LitMax(%d, %d, %d, 3) = %d, want %d" lo hi div got want))))))
        (recur)))))

(deftest bigmin-litmax-2d-frng
  (run-frng-sim! bigmin-litmax-2d-sim {:attempts 30 :max-size 8192}))

(deftest bigmin-litmax-3d-frng
  (run-frng-sim! bigmin-litmax-3d-sim {:attempts 30 :max-size 8192}))

(deftest dim-mask-basic
  ;; x-mask-2d is public-adjacent — re-derive here to avoid leaking internals.
  (is (= 0x5555555555555555 (#'z/dim-mask 0 2)))
  (is (= (bit-not 0x5555555555555555) (#'z/dim-mask 1 2)))
  (is (= 0x1249249249249249 (#'z/dim-mask 0 3))))

(deftest encode-2-interleaving
  (is (= 0 (z/encode-2 0 0)))
  (is (= -1 (z/encode-2 0xFFFFFFFF 0xFFFFFFFF)))
  (is (= 0x5555555555555555 (z/encode-2 0xFFFFFFFF 0))))

(defn- naive-encode-2 [^long x ^long y]
  ;; Reference implementation: set each input bit in its interleaved position.
  (loop [i 0 acc 0]
    (if (>= i 32)
      acc
      (recur (inc i)
             (bit-or acc
                     (bit-shift-left (bit-and (unsigned-bit-shift-right x i) 1)
                                     (* 2 i))
                     (bit-shift-left (bit-and (unsigned-bit-shift-right y i) 1)
                                     (inc (* 2 i))))))))

(defn- naive-encode-3 [^long x ^long y ^long z]
  (loop [i 0 acc 0]
    (if (>= i 21)
      acc
      (recur (inc i)
             (bit-or acc
                     (bit-shift-left (bit-and (unsigned-bit-shift-right x i) 1)
                                     (* 3 i))
                     (bit-shift-left (bit-and (unsigned-bit-shift-right y i) 1)
                                     (inc (* 3 i)))
                     (bit-shift-left (bit-and (unsigned-bit-shift-right z i) 1)
                                     (+ 2 (* 3 i))))))))

(deftest encode-matches-naive
  ;; Validate the 64-bit magic-number derivations against a bit-by-bit reference.
  (doseq [[x y] [[0 0] [1 0] [0 1] [0xFFFF 0xFFFF]
                 [0xDEADBEEF 0xCAFEBABE]
                 [0xFFFFFFFF 0] [0 0xFFFFFFFF]
                 [0xFFFFFFFF 0xFFFFFFFF]]]
    (is (= (naive-encode-2 x y) (z/encode-2 x y))
        (format "encode-2(%x, %x)" x y)))
  (doseq [[x y zc] [[0 0 0] [1 0 0] [0 1 0] [0 0 1]
                    [0x1FFFFF 0x1FFFFF 0x1FFFFF]
                    [0x12345 0x67890 0xABCDE]]]
    (is (= (naive-encode-3 x y zc) (z/encode-3 x y zc))
        (format "encode-3(%x, %x, %x)" x y zc))))

(deftest encode-2-high-bits
  ;; A coordinate with bits above the old 16-bit range must round-trip cleanly.
  (let [x 0x01234567
        y 0x89ABCDEF
        code (z/encode-2 x y)]
    (is (= x (z/decode-2-x code)))
    (is (= y (z/decode-2-y code)))))
