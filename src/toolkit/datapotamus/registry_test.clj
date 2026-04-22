(ns toolkit.datapotamus.registry-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.datapotamus.registry :as reg]))

(deftest install-returns-prior-and-replaces
  (let [r (reg/make)
        a {:run-id (random-uuid)} b {:run-id (random-uuid)}]
    (is (nil? (reg/install! r "s" a)))
    (is (= a (reg/install! r "s" b)))
    (is (= b (reg/current r "s")))))

(deftest remove-only-if-matches
  (let [r (reg/make) rid (random-uuid)]
    (reg/install! r "s" {:run-id rid})
    (reg/remove-if-matches! r "s" (random-uuid))
    (is (= rid (:run-id (reg/current r "s"))))
    (reg/remove-if-matches! r "s" rid)
    (is (nil? (reg/current r "s")))))
