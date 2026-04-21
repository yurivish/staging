(ns toolkit.h2-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.h2 :as h2]))

(defn- assert-in-bin [value lower width]
  (is (<= lower value))
  (is (< value (+ lower width))))

(deftest encode-decode-zero
  (let [a 2, b 3
        idx (h2/encode a b 0)]
    (is (= 0 (h2/decode-lower a b idx)))
    (is (= 4 (h2/decode-width a b idx)))))

(deftest encode-decode-small-value
  (let [a 2, b 3, v 10
        idx (h2/encode a b v)]
    (assert-in-bin v (h2/decode-lower a b idx) (h2/decode-width a b idx))))

(deftest encode-decode-large-value
  (let [a 4, b 4, v 100000
        idx (h2/encode a b v)]
    (assert-in-bin v (h2/decode-lower a b idx) (h2/decode-width a b idx))))

(deftest multiple-values
  (let [a 2, b 3]
    (doseq [v [0 1 10 100 1000]]
      (let [idx (h2/encode a b v)]
        (assert-in-bin v (h2/decode-lower a b idx) (h2/decode-width a b idx))))))
