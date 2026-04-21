(ns toolkit.exhaustigen-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.exhaustigen :as ex]))

(defn- count-cases [f]
  (let [g (ex/make)]
    (loop [n 0]
      (if (ex/done!? g)
        n
        (do (f g) (recur (inc n)))))))

(deftest elts-count
  (is (= (+ (* 5 5 5) (* 5 5) 5 1)
         (count-cases #(ex/elts % 3 4)))))

(deftest comb-count
  (is (= (+ (* 5 5 5 5 5) (* 5 5 5 5) (* 5 5 5) (* 5 5) 5 1)
         (count-cases #(ex/comb % 5)))))

(deftest perm-count
  (is (= (* 5 4 3 2 1)
         (count-cases #(ex/perm % 5)))))

(deftest subset-count
  (is (= (bit-shift-left 1 5)
         (count-cases #(ex/subset % 5)))))
