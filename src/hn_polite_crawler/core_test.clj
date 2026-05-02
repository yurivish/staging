(ns hn-polite-crawler.core-test
  "Unit + E2E tests for the polite Algolia crawler. Stubs all HTTP."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-polite-crawler.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- Unit: pagination loop --------------------------------------------------

(deftest fetch-paginates-until-empty-hits
  (let [pages {0 {:hits [{:objectID "a"} {:objectID "b"}] :nbPages 3}
               1 {:hits [{:objectID "c"}]                  :nbPages 3}
               2 {:hits []                                  :nbPages 3}}
        calls (atom [])]
    (with-redefs [core/fetch-page! (fn [{:keys [page]}]
                                     (swap! calls conj page)
                                     (or (get pages page)
                                         (throw (ex-info "missing page" {:page page}))))]
      (let [result (#'core/crawl-user! "alice")]
        (is (= 3 (count (:history result))))
        (is (= "alice" (:user result)))
        (is (= [0 1 2] @calls))))))

;; --- E2E: all users succeed -------------------------------------------------

(deftest end-to-end-all-succeed
  (let [pages {"alice"  {0 {:hits [{:objectID "a1"} {:objectID "a2"}] :nbPages 1}}
               "bob"    {0 {:hits [{:objectID "b1"}]                   :nbPages 1}}
               "carol"  {0 {:hits []                                    :nbPages 1}}}]
    (with-redefs [core/fetch-page! (fn [{:keys [username page]}]
                                     (or (get-in pages [username page])
                                         (throw (ex-info "missing" {:user username :page page}))))]
      (let [res (flow/run-seq
                 (core/build-flow ["alice" "bob" "carol"]
                                  {:k 2 :rps 1000 :burst 1000
                                   :max-retries 0 :base-ms 1})
                 [:tick])
            outs (vec (mapcat identity (:outputs res)))
            by-user (into {} (map (juxt :user identity)) outs)]
        (is (= :completed (:state res)))
        (is (= 3 (count outs)))
        (is (= 2 (count (:history (get by-user "alice")))))
        (is (= 1 (count (:history (get by-user "bob")))))
        (is (= 0 (count (:history (get by-user "carol")))))
        (is (every? #(not (:dead-letter? %)) outs))))))

;; --- E2E: persistent failure → dead-letter ---------------------------------

(deftest end-to-end-persistent-429-dead-letters
  (let [calls (atom 0)]
    (with-redefs [core/fetch-page! (fn [_]
                                     (swap! calls inc)
                                     (throw (ex-info "rate-limited"
                                                     {:transient true :status 429})))]
      (let [res (flow/run-seq
                 (core/build-flow ["doomed"]
                                  {:k 1 :rps 1000 :burst 1000
                                   :max-retries 2 :base-ms 1})
                 [:tick])
            out (-> res :outputs first first)]
        (is (= :completed (:state res)))
        (is (true? (:dead-letter? out)))
        (is (= 3 @calls) "1 initial + 2 retries before dead-letter")
        (is (= "doomed" (-> out :data :user)))))))

;; --- E2E: rate-limit applies globally across pool --------------------------

(deftest end-to-end-rate-limit-bounds-global-rps
  ;; 12 users, K=4 workers, rate=10 RPS, burst=2.
  ;; Each user makes exactly 1 fetch (single page, no hits).
  ;; Expected floor: (12 - 2) / 10 = 1.0 sec.
  (with-redefs [core/fetch-page! (fn [_]
                                   (Thread/sleep 5)  ;; small per-call work
                                   {:hits [] :nbPages 1})]
    (let [users (mapv #(str "user" %) (range 12))
          t0    (System/currentTimeMillis)
          res   (flow/run-seq
                 (core/build-flow users
                                  {:k 4 :rps 10 :burst 2
                                   :max-retries 0 :base-ms 1})
                 [:tick])
          ms    (- (System/currentTimeMillis) t0)
          outs  (vec (mapcat identity (:outputs res)))]
      (is (= :completed (:state res)))
      (is (= 12 (count outs)))
      (is (>= ms 950)
          (str "rate-limit didn't apply globally; ms=" ms " (expected ≥ ~1000ms)")))))
