(ns toolkit.datapotamus.backoff-test
  "Tests for `c/with-backoff` — decorator that retries an inner
   handler-map's `:on-data` on errors matching a predicate, with
   exponential backoff + jitter, and routes persistent failures to
   a `:dead-letter` port."
  (:refer-clojure :exclude [run!])
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [toolkit.datapotamus.combinators :as c]
            [toolkit.datapotamus.flow :as flow]
            [toolkit.datapotamus.msg :as msg]
            [toolkit.datapotamus.step :as step]))

(defn- mk-flaky-handler
  "Returns [handler attempts-atom]. Handler succeeds after N failures
   for any input, then continues succeeding. Counts attempts per data."
  [n-failures]
  (let [attempts (atom {})]
    [(step/handler-map
      {:ports {:ins {:in ""} :outs {:out ""}}
       :on-data
       (fn [ctx _ d]
         (let [n (get (swap! attempts update d (fnil inc 0)) d)]
           (if (<= n n-failures)
             (throw (ex-info "transient" {:transient true :attempt n}))
             {:out [(msg/child ctx {:input d :attempts n})]})))})
     attempts]))

;; ============================================================================
;; A. Success on first try — no retry
;; ============================================================================

(deftest a1-no-retry-when-no-error
  (let [[h _] (mk-flaky-handler 0)
        wf  (c/with-backoff {:max-retries 3 :base-ms 10
                             :retry? (comp :transient ex-data)}
                            h)
        res (flow/run-seq wf [42])]
    (is (= :completed (:state res)))
    (is (= [[{:input 42 :attempts 1}]] (:outputs res)))))

;; ============================================================================
;; B. Recovers within max-retries
;; ============================================================================

(deftest b1-retries-then-succeeds
  ;; Fails 2 times, succeeds on 3rd. max-retries=3 allows 3 retries → 4 total tries.
  (let [[h attempts] (mk-flaky-handler 2)
        wf  (c/with-backoff {:max-retries 3 :base-ms 5
                             :retry? (comp :transient ex-data)}
                            h)
        res (flow/run-seq wf [7])]
    (is (= :completed (:state res)))
    (is (= [[{:input 7 :attempts 3}]] (:outputs res)))
    (is (= 3 (get @attempts 7)))))

;; ============================================================================
;; C. Backoff timing — exponential with floor
;; ============================================================================

(deftest c1-elapsed-time-respects-backoff
  ;; base-ms=50, attempts 1 fails (sleep 50), attempt 2 fails (sleep 100),
  ;; attempt 3 succeeds. Floor: 150ms. Allow some jitter slack on top.
  (let [[h _] (mk-flaky-handler 2)
        wf  (c/with-backoff {:max-retries 3 :base-ms 50
                             :jitter 0  ;; deterministic
                             :retry? (comp :transient ex-data)}
                            h)
        t0  (System/currentTimeMillis)
        res (flow/run-seq wf [9])
        ms  (- (System/currentTimeMillis) t0)]
    (is (= :completed (:state res)))
    (is (>= ms 150) (str "elapsed " ms "ms < 150ms floor (no backoff?)"))
    (is (< ms 400) (str "elapsed " ms "ms — too much, jitter or extra retries"))))

;; ============================================================================
;; D. Exhausts retries → dead-letter (flagged on :out)
;; ============================================================================

(deftest d1-dead-letters-after-max-retries
  ;; Always fails. max-retries=2 → 3 attempts total → dead-letter on :out.
  (let [[h attempts] (mk-flaky-handler 1000)
        wf  (c/with-backoff {:max-retries 2 :base-ms 1
                             :retry? (comp :transient ex-data)}
                            h)
        res (flow/run-seq wf [99])
        out (-> res :outputs first first)]
    (is (= :completed (:state res)))
    (testing "single dead-letter emission, no successful :out"
      (is (= 1 (count (first (:outputs res)))))
      (is (true? (:dead-letter? out)))
      (is (= 99 (:data out)))
      (is (= 3 (:attempts out)) "1 initial + 2 retries")
      (is (= 3 (get @attempts 99))))))

;; ============================================================================
;; E. Non-matching error → no retry, dead-letter immediately
;; ============================================================================

(deftest e1-non-retryable-error-fails-fast
  (let [attempts (atom 0)
        h (step/handler-map
           {:ports {:ins {:in ""} :outs {:out ""}}
            :on-data
            (fn [_ctx _ _d]
              (swap! attempts inc)
              (throw (ex-info "permanent" {:permanent true})))})
        wf  (c/with-backoff {:max-retries 5 :base-ms 1
                             :retry? (comp :transient ex-data)}  ;; only retry :transient
                            h)
        res (flow/run-seq wf [1])
        out (-> res :outputs first first)]
    (is (= 1 @attempts) "exactly one attempt; non-matching error not retried")
    (is (true? (:dead-letter? out)))
    (is (= 1 (:attempts out)))))

;; ============================================================================
;; F. Property — attempts <= max-retries + 1
;; ============================================================================

(defspec p1-bounded-attempts 20
  (prop/for-all [max-retries  (gen/choose 0 4)
                 always-fail? gen/boolean]
    (let [attempts (atom 0)
          h (step/handler-map
             {:ports {:ins {:in ""} :outs {:out ""}}
              :on-data
              (fn [ctx _ _d]
                (swap! attempts inc)
                (if always-fail?
                  (throw (ex-info "x" {:transient true}))
                  {:out [(msg/child ctx :ok)]}))})
          wf  (c/with-backoff {:max-retries max-retries :base-ms 1
                               :retry? (comp :transient ex-data)}
                              h)
          res (flow/run-seq wf [:tick])
          out (-> res :outputs first first)]
      (and (= :completed (:state res))
           (<= @attempts (inc max-retries))
           (if always-fail?
             (and (= @attempts (inc max-retries))
                  (true? (:dead-letter? out)))
             (and (= @attempts 1)
                  (= :ok out)))))))
