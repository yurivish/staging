(ns toolkit.datapotamus2.token
  "XOR tokens for fan-out/fan-in coordination.

   A token is a 64-bit long. A token map is {token-id → long}. Merging
   two token maps XORs values with matching keys. Splitting a token value
   v into n returns n values that XOR back to v."
  (:import [java.security SecureRandom]))

(def ^:private ^SecureRandom rng (SecureRandom.))

(defn- rand-token [] (.nextLong rng))

(defn split-value
  "Split v into n values whose XOR equals v."
  [v n]
  (when (<= n 0)
    (throw (IllegalArgumentException. "must split into at least one value")))
  (let [randoms (repeatedly (dec n) rand-token)]
    (conj (vec randoms) (reduce bit-xor v randoms))))

(defn merge-tokens
  "XOR-merge two token maps."
  [a b]
  (merge-with bit-xor a b))

(defn split-tokens
  "Split each token in the map into n subtokens. Returns a vector of n
   token maps whose pointwise XOR equals the input map."
  [tokens n]
  (let [split-per-key (update-vals tokens #(split-value % n))]
    (mapv (fn [i] (update-vals split-per-key #(nth % i)))
          (range n))))
