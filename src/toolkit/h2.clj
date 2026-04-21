(ns toolkit.h2
  "Histogram bin encoding — maps a non-negative long value to a compact bin
   index. Ported from toolkit/h2/h2.go.

   Parameters `a` and `b` define the layout:
     - Values below 2^(a+b+1) fall in a linear section with bin width 2^a.
     - Above that, each binary log-segment is divided into 2^b bins.

   Encode is a handful of primitive ops with no allocation. Decode is split
   into `decode-lower` and `decode-width` so callers that only need one of
   the two can avoid the vector allocation a combined fn would require; the
   shared branch logic is cheap to recompute.

   Realistic configurations keep `a+b+1` comfortably below 63, so signed-long
   arithmetic matches Go's uint64 semantics. If `a+b+1 >= 63` the comparison
   `value < 1<<c` would need `Long/compareUnsigned` — not paid for here.")

(defn encode
  "Encode a non-negative long `value` to its bin index under params `a`, `b`."
  ^long [^long a ^long b ^long value]
  (let [c (unchecked-add (unchecked-add a b) 1)]
    (if (< value (bit-shift-left 1 c))
      (unsigned-bit-shift-right value a)
      (let [log-segment (unchecked-subtract (unchecked-subtract 64 (Long/numberOfLeadingZeros value)) 1)]
        (unchecked-add
          (unsigned-bit-shift-right value (unchecked-subtract log-segment b))
          (bit-shift-left (unchecked-add (unchecked-subtract log-segment c) 1) b))))))

(defn decode-lower
  "Lower edge (inclusive) of the bin at `index`."
  ^long [^long a ^long b ^long index]
  (let [c                 (unchecked-add (unchecked-add a b) 1)
        bins-below-cutoff (bit-shift-left 1 (unchecked-subtract c a))]
    (if (< index bins-below-cutoff)
      (bit-shift-left index a)
      (let [log-segment (unchecked-add c (unsigned-bit-shift-right (unchecked-subtract index bins-below-cutoff) b))
            offset      (bit-and index (unchecked-subtract (bit-shift-left 1 b) 1))]
        (unchecked-add
          (bit-shift-left 1 log-segment)
          (bit-shift-left offset (unchecked-subtract log-segment b)))))))

(defn decode-width
  "Width of the bin at `index`. The bin covers `[decode-lower, decode-lower + decode-width)`."
  ^long [^long a ^long b ^long index]
  (let [c                 (unchecked-add (unchecked-add a b) 1)
        bins-below-cutoff (bit-shift-left 1 (unchecked-subtract c a))]
    (if (< index bins-below-cutoff)
      (bit-shift-left 1 a)
      (let [log-segment (unchecked-add c (unsigned-bit-shift-right (unchecked-subtract index bins-below-cutoff) b))]
        (bit-shift-left 1 (unchecked-subtract log-segment b))))))
