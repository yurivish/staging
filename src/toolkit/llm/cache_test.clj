(ns toolkit.llm.cache-test
  "Round-trip tests for toolkit.llm.cache. Uses a fresh LMDB env per test
   in a tmpdir; no network."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [toolkit.llm.cache :as cache]))

(defn- tmpdir []
  (let [d (io/file (System/getProperty "java.io.tmpdir")
                   (str "llm-cache-test-" (System/nanoTime)))]
    (.mkdirs d)
    d))

(defn- with-store [f]
  (let [d (tmpdir)
        s (cache/open (.getPath d) {:map-size (* 16 1024 1024)})]
    (try (f s)
         (finally
           (cache/close s)
           (doseq [^java.io.File f (reverse (file-seq d))] (.delete f))))))

;; --- key derivation --------------------------------------------------------

(deftest sha256-deterministic
  (is (= (seq (cache/sha256-bytes "x")) (seq (cache/sha256-bytes "x"))))
  (is (not= (seq (cache/sha256-bytes "x")) (seq (cache/sha256-bytes "y"))))
  (is (= 32 (alength (cache/sha256-bytes "x")))))

;; --- bytes round-trip ------------------------------------------------------

(deftest bytes-roundtrip
  (with-store
    (fn [s]
      (let [k (cache/key-bytes-of "k1")
            v (.getBytes "hello" "UTF-8")]
        (is (nil? (cache/get-bytes s k)))
        (cache/put-bytes! s k v)
        (is (= "hello" (String. (cache/get-bytes s k) "UTF-8")))))))

;; --- json round-trip -------------------------------------------------------

(deftest json-roundtrip
  (with-store
    (fn [s]
      (let [k (cache/key-bytes-of "obj-key")
            v {:x 1 :ys [1 2 3] :name "foo"}]
        (is (nil? (cache/get-json s k)))
        (cache/put-json! s k v)
        (is (= v (cache/get-json s k)))))))

;; --- compute! semantics ----------------------------------------------------

(deftest compute-miss-then-hit
  (with-store
    (fn [s]
      (let [k    (cache/key-bytes-of "compute-1")
            calls (atom 0)
            produce #(do (swap! calls inc) {:result %&})
            r1   (cache/compute! s k produce)
            r2   (cache/compute! s k produce)]
        (is (= 1 @calls) "produce should run only on miss")
        (is (= :miss (:cache r1)))
        (is (= :hit  (:cache r2)))
        (is (= (:value r1) (:value r2)) "cached value matches first compute")))))

(deftest compute-distinct-keys-distinct-values
  (with-store
    (fn [s]
      (let [a (cache/compute! s (cache/key-bytes-of "a") (fn [] {:k :a}))
            b (cache/compute! s (cache/key-bytes-of "b") (fn [] {:k :b}))]
        (is (= {:k :a} (:value a)))
        (is (= {:k :b} (:value b)))
        (is (every? #(= :miss %) [(:cache a) (:cache b)]))))))

(deftest compute-survives-restart
  (let [d (tmpdir)
        k (cache/key-bytes-of "persist")]
    (try
      (let [s (cache/open (.getPath d) {:map-size (* 16 1024 1024)})]
        (try
          (cache/compute! s k (fn [] {:n 42}))
          (finally (cache/close s))))
      (let [s (cache/open (.getPath d) {:map-size (* 16 1024 1024)})]
        (try
          (let [r (cache/compute! s k (fn [] {:n -1}))]
            (is (= :hit (:cache r)) "value persists across env close/open")
            (is (= {:n 42} (:value r))))
          (finally (cache/close s))))
      (finally
        (doseq [^java.io.File f (reverse (file-seq d))] (.delete f))))))

;; --- through! semantics (default store + bypass) ----------------------------

(defn- with-temp-default-store
  "Redirect the default store to a fresh tmpdir for the duration of `f`."
  [f]
  (let [d (tmpdir)
        s (cache/open (.getPath d) {:map-size (* 16 1024 1024)})]
    (with-redefs [cache/default-store (constantly s)]
      (try (f s)
           (finally
             (cache/close s)
             (doseq [^java.io.File f (reverse (file-seq d))] (.delete f)))))))

(deftest through-miss-then-hit
  (with-temp-default-store
    (fn [_s]
      (let [calls (atom 0)
            r1    (cache/through! "k-1" (fn [] (swap! calls inc) {:answer 42}))
            r2    (cache/through! "k-1" (fn [] (swap! calls inc) {:answer -1}))]
        (is (= 1 @calls) "produce should run only on miss")
        (is (= :miss (:cache r1)))
        (is (= :hit  (:cache r2)))
        (is (= {:answer 42} (:value r1)))
        (is (= {:answer 42} (:value r2)))))))

(deftest through-distinct-keys
  (with-temp-default-store
    (fn [_s]
      (let [a (cache/through! "k-a" (fn [] {:k :a}))
            b (cache/through! "k-b" (fn [] {:k :b}))]
        (is (= {:k :a} (:value a)))
        (is (= {:k :b} (:value b)))
        (is (every? #(= :miss %) [(:cache a) (:cache b)]))))))

(deftest through-nil-not-cached
  (with-temp-default-store
    (fn [_s]
      (let [calls (atom 0)
            f #(do (swap! calls inc) nil)
            r1 (cache/through! "k" f)
            r2 (cache/through! "k" f)]
        (is (= 2 @calls) "nil produce result is not cached → both calls run")
        (is (= :miss (:cache r1)))
        (is (= :miss (:cache r2)))
        (is (nil? (:value r1)))))))

