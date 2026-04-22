(ns toolkit.datapotamus2.token-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.datapotamus2.token :as tok]))

(deftest split-value-xors-back
  (doseq [n [1 2 3 5 17]]
    (let [vs (tok/split-value 42 n)]
      (is (= n (count vs)))
      (is (= 42 (reduce bit-xor vs))))))

(deftest split-zero-xors-to-zero
  (let [vs (tok/split-value 0 3)]
    (is (= 0 (reduce bit-xor vs)))))

(deftest merge-tokens-xors-per-key
  (is (= {:a 6 :b 5} (tok/merge-tokens {:a 3 :b 5} {:a 5}))))

(deftest split-tokens-xors-back-per-key
  (let [input  {:g1 7 :g2 11}
        splits (tok/split-tokens input 3)
        merged (reduce tok/merge-tokens {} splits)]
    (is (= 3 (count splits)))
    (is (= input merged))))
