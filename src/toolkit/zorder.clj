(ns toolkit.zorder
  "Morton code (Z-order curve) encoding/decoding and 2D bounding-box range
   splitting. Ported from zorder.go at the repo root; originally from C code
   by Fabian Giesen (https://fgiesen.wordpress.com/2009/12/13/decoding-morton-codes/).

   Layout: 2D is `yxyxyxyx`, 3D is `zyxzyxzyxzyx`.

   The Go source uses `uint32` codes; this port widens to 64-bit `long`:
     - `encode-2` interleaves two 32-bit coordinates into a 64-bit code.
     - `encode-3` interleaves three 21-bit coordinates into a 63-bit code.
   Where `litmax-bigmin-2d` and `range-contained-in-bbox-2d?` compare codes,
   `Long/compareUnsigned` is used so the comparisons agree with Go's `uint32`
   semantics extended to 64 bits.

   LITMAX/BIGMIN are Tropf's algorithms for locating the next/previous Morton
   code inside a query rectangle — see Tropf & Herzog, \"Multidimensional Range
   Search in Dynamically Balanced Trees\", Angewandte Informatik, Feb 1981.")

;; Dimension masks. y-mask-2d exceeds Long/MAX_VALUE as written, so derive it
;; by complementing x-mask-2d.
(def ^:private ^:const x-mask-2d 0x5555555555555555)
(def ^:private y-mask-2d (bit-not x-mask-2d))

(def ^:private ^:const x-mask-3d 0x1249249249249249)
(def ^:private ^:const y-mask-3d 0x2492492492492492)
(def ^:private ^:const z-mask-3d 0x4924924924924924)

;; Insert one 0 bit between each of the low 32 bits of x.
(defn- part1-by-1
  ^long [^long x]
  (let [x (bit-and x 0x00000000ffffffff)
        x (bit-and (bit-xor x (bit-shift-left x 16)) 0x0000ffff0000ffff)
        x (bit-and (bit-xor x (bit-shift-left x 8))  0x00ff00ff00ff00ff)
        x (bit-and (bit-xor x (bit-shift-left x 4))  0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-xor x (bit-shift-left x 2))  0x3333333333333333)
        x (bit-and (bit-xor x (bit-shift-left x 1))  0x5555555555555555)]
    x))

;; Insert two 0 bits between each of the low 21 bits of x.
(defn- part1-by-2
  ^long [^long x]
  (let [x (bit-and x 0x00000000001fffff)
        x (bit-and (bit-xor x (bit-shift-left x 32)) 0x001f00000000ffff)
        x (bit-and (bit-xor x (bit-shift-left x 16)) 0x001f0000ff0000ff)
        x (bit-and (bit-xor x (bit-shift-left x 8))  0x100f00f00f00f00f)
        x (bit-and (bit-xor x (bit-shift-left x 4))  0x10c30c30c30c30c3)
        x (bit-and (bit-xor x (bit-shift-left x 2))  0x1249249249249249)]
    x))

;; Inverse of part1-by-1: delete every odd-indexed bit.
(defn- compact1-by-1
  ^long [^long x]
  (let [x (bit-and x 0x5555555555555555)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 1))  0x3333333333333333)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 2))  0x0f0f0f0f0f0f0f0f)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 4))  0x00ff00ff00ff00ff)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 8))  0x0000ffff0000ffff)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 16)) 0x00000000ffffffff)]
    x))

;; Inverse of part1-by-2: keep only bits at positions divisible by 3.
(defn- compact1-by-2
  ^long [^long x]
  (let [x (bit-and x 0x1249249249249249)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 2))  0x10c30c30c30c30c3)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 4))  0x100f00f00f00f00f)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 8))  0x001f0000ff0000ff)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 16)) 0x001f00000000ffff)
        x (bit-and (bit-xor x (unsigned-bit-shift-right x 32)) 0x00000000001fffff)]
    x))

(defn encode-2
  "Encode two 32-bit coordinates into a 64-bit 2D Morton code."
  ^long [^long x ^long y]
  (bit-or (bit-shift-left (part1-by-1 y) 1) (part1-by-1 x)))

(defn encode-3
  "Encode three 21-bit coordinates into a 63-bit 3D Morton code."
  ^long [^long x ^long y ^long z]
  (bit-or (bit-shift-left (part1-by-2 z) 2)
          (bit-shift-left (part1-by-2 y) 1)
          (part1-by-2 x)))

(defn decode-2-x ^long [^long code] (compact1-by-1 code))
(defn decode-2-y ^long [^long code] (compact1-by-1 (unsigned-bit-shift-right code 1)))

(defn decode-3-x ^long [^long code] (compact1-by-2 code))
(defn decode-3-y ^long [^long code] (compact1-by-2 (unsigned-bit-shift-right code 1)))
(defn decode-3-z ^long [^long code] (compact1-by-2 (unsigned-bit-shift-right code 2)))

(defn- well-formed-bbox!
  "Throw IllegalArgumentException if br precedes tl in either x or y."
  [^long tl ^long br]
  (when (or (neg? (Long/compareUnsigned (bit-and br x-mask-2d) (bit-and tl x-mask-2d)))
            (neg? (Long/compareUnsigned (bit-and br y-mask-2d) (bit-and tl y-mask-2d))))
    (throw (IllegalArgumentException.
            "bottom-right may not precede top-left in either the x or y dimension"))))

(defn- range-contained-in-bbox-2d?
  "True iff the Morton range [tl, br] is fully contained in its 2D bounding box."
  [^long tl ^long br]
  (well-formed-bbox! tl br)
  (let [width  (unchecked-inc (unchecked-subtract (decode-2-x br) (decode-2-x tl)))
        height (unchecked-inc (unchecked-subtract (decode-2-y br) (decode-2-y tl)))
        count  (unchecked-multiply width height)]
    (neg? (Long/compareUnsigned (unchecked-subtract br tl) count))))

(defn litmax-bigmin-2d
  "Split a 2D Morton range at the MSB difference between min and max.
   Returns [litmax bigmin] such that [min, max] splits into [min, litmax] and
   [bigmin, max]. If min = max, both are returned unchanged."
  [^long min ^long max]
  (cond
    (= min max) [min max]
    :else
    (let [[min max] (if (pos? (Long/compareUnsigned min max)) [max min] [min max])
          min      (long min)
          max      (long max)
          diff     (bit-xor min max)
          diff-msb (bit-shift-left 1 (unchecked-subtract 63 (Long/numberOfLeadingZeros diff)))
          split-x? (not (zero? (bit-and diff-msb x-mask-2d)))
          split-mask (if split-x? x-mask-2d y-mask-2d)
          major-mask (bit-and (unchecked-dec diff-msb) split-mask)
          minor-mask (bit-and-not (unchecked-dec diff-msb) split-mask)
          common   (bit-and-not min (unchecked-add diff-msb (unchecked-dec diff-msb)))
          litmax   (bit-or common major-mask (bit-and minor-mask max))
          bigmin   (bit-or common diff-msb (bit-and minor-mask min))]
      [litmax bigmin])))

(defn- dim-mask
  "Bitmask selecting every bit position belonging to dimension `dim`
   (0-indexed) in an `ndims`-dimensional Morton code."
  ^long [^long dim ^long ndims]
  (case (int ndims)
    2 (case (int dim) 0 x-mask-2d 1 y-mask-2d)
    3 (case (int dim) 0 x-mask-3d 1 y-mask-3d 2 z-mask-3d)
    (loop [i dim mask 0]
      (if (>= i 64)
        mask
        (recur (unchecked-add i ndims) (bit-or mask (bit-shift-left 1 i)))))))

;; Tropf's LOAD_xxx10000: set `bit` in val, clear all lower bits of the same
;; dimension (identified by dm).
(defn- load-xxx-10000
  ^long [^long val ^long bit ^long dm]
  (-> val
      (bit-or bit)
      (bit-and-not (bit-and (unchecked-dec bit) dm))))

;; Tropf's LOAD_xxx01111: clear `bit` in val, set all lower bits of the same
;; dimension.
(defn- load-xxx-01111
  ^long [^long val ^long bit ^long dm]
  (-> val
      (bit-and-not bit)
      (bit-or (bit-and (unchecked-dec bit) dm))))

;; Decision tables for `litmax` and `bigmin` below are verified case-for-case
;; against Tropf's Pascal source at
;; refs/tropf_litmax_bigmin DBCode_mit_Erlaeuterung.txt.

(defn litmax
  "Largest Morton code in the search rectangle [min, max] whose Z-value is
   less than `div`. `ndims` is the number of interleaved dimensions.

   Precondition: Z(min) < Z(div) < Z(max), where min and max are the
   minimum-coordinate and maximum-coordinate corners of the search rectangle."
  ^long [^long min ^long max ^long div ^long ndims]
  (let [masks (long-array 8)]
    (dotimes [d ndims]
      (aset masks d (dim-mask d ndims)))
    (loop [pos 63 min min max max litmax max]
      (if (neg? pos)
        litmax
        (let [bit (bit-shift-left 1 pos)
              dm  (aget masks (rem pos ndims))
              sel (bit-or
                   (if (zero? (bit-and div bit)) 0 4)
                   (if (zero? (bit-and min bit)) 0 2)
                   (if (zero? (bit-and max bit)) 0 1))]
          (case sel
            0 (recur (unchecked-dec pos) min max litmax)                    ; 0,0,0
            1 (recur (unchecked-dec pos) min (load-xxx-01111 max bit dm) litmax) ; 0,0,1
            3 litmax                                                         ; 0,1,1
            4 max                                                            ; 1,0,0
            5 (recur (unchecked-dec pos)                                     ; 1,0,1
                     (load-xxx-10000 min bit dm)
                     max
                     (load-xxx-01111 max bit dm))
            7 (recur (unchecked-dec pos) min max litmax)                    ; 1,1,1
            (recur (unchecked-dec pos) min max litmax)))))))

(defn bigmin
  "Smallest Morton code in the search rectangle [min, max] whose Z-value is
   greater than `div`. `ndims` is the number of interleaved dimensions."
  ^long [^long min ^long max ^long div ^long ndims]
  (let [masks (long-array 8)]
    (dotimes [d ndims]
      (aset masks d (dim-mask d ndims)))
    (loop [pos 63 min min max max bigmin min]
      (if (neg? pos)
        bigmin
        (let [bit (bit-shift-left 1 pos)
              dm  (aget masks (rem pos ndims))
              sel (bit-or
                   (if (zero? (bit-and div bit)) 0 4)
                   (if (zero? (bit-and min bit)) 0 2)
                   (if (zero? (bit-and max bit)) 0 1))]
          (case sel
            0 (recur (unchecked-dec pos) min max bigmin)                    ; 0,0,0
            1 (recur (unchecked-dec pos)                                    ; 0,0,1
                     min
                     (load-xxx-01111 max bit dm)
                     (load-xxx-10000 min bit dm))
            3 min                                                            ; 0,1,1
            4 bigmin                                                         ; 1,0,0
            5 (recur (unchecked-dec pos)                                    ; 1,0,1
                     (load-xxx-10000 min bit dm)
                     max
                     bigmin)
            7 (recur (unchecked-dec pos) min max bigmin)                    ; 1,1,1
            (recur (unchecked-dec pos) min max bigmin)))))))

(defn split-bbox-2d
  "Decompose a 2D bounding box (given by top-left and bottom-right Morton
   codes) into a flat vector of [lo hi lo hi ...] Morton sub-range pairs.
   Throws if br precedes tl in either dimension."
  [^long tl ^long br]
  (well-formed-bbox! tl br)
  (loop [stack [[tl br]]
         out   (transient [])]
    (if-let [top (peek stack)]
      (let [lo    (long (nth top 0))
            hi    (long (nth top 1))
            stack (pop stack)]
        (if (range-contained-in-bbox-2d? lo hi)
          (let [n (count out)]
            (if (and (>= n 2)
                     (= (unchecked-inc (long (nth out (unchecked-dec n)))) lo))
              (recur stack (assoc! out (unchecked-dec n) hi))
              (recur stack (-> out (conj! lo) (conj! hi)))))
          (let [[litmax bigmin] (litmax-bigmin-2d lo hi)]
            (recur (-> stack (conj [bigmin hi]) (conj [lo litmax]))
                   out))))
      (persistent! out))))
