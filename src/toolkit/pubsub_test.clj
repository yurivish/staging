(ns toolkit.pubsub-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.core.async :as async]
            [toolkit.pubsub :as ps]))

(deftest basic-pub-sub
  (let [pubsub   (ps/make)
        received (atom [])
        unsub    (ps/sub pubsub ["foo" :*]
                         (fn [subj msg _] (swap! received conj [subj msg])))]
    (ps/pub pubsub ["foo" "bar"] :m1)
    (ps/pub pubsub ["foo" "baz"] :m2)
    (ps/pub pubsub ["other"]     :m3)
    (is (= [[["foo" "bar"] :m1] [["foo" "baz"] :m2]] @received))
    (unsub)
    (ps/pub pubsub ["foo" "bar"] :m4)
    (is (= 2 (count @received)) "unsub stops further delivery")))

(deftest fwc-pattern
  (let [pubsub (ps/make)
        got    (atom [])]
    (ps/sub pubsub ["foo" :>] (fn [s _ _] (swap! got conj s)))
    (ps/pub pubsub ["foo" "a" "b"] 1)
    (ps/pub pubsub ["foo" "x"]     2)
    (ps/pub pubsub ["bar"]         3)
    (is (= [["foo" "a" "b"] ["foo" "x"]] @got))))

(deftest queue-group-load-balancing
  (let [pubsub (ps/make)
        a      (atom 0)
        b      (atom 0)]
    (ps/sub pubsub ["work"] (fn [_ _ _] (swap! a inc)) {:queue "g"})
    (ps/sub pubsub ["work"] (fn [_ _ _] (swap! b inc)) {:queue "g"})
    (dotimes [_ 200] (ps/pub pubsub ["work"] :task))
    (is (= 200 (+ @a @b)) "exactly one queue member fires per pub")
    ;; Probability both land on the same member across 200 rolls is ~(0.5)^199,
    ;; so flakiness here would mean something is broken, not unlucky.
    (is (pos? @a))
    (is (pos? @b))))

(deftest plain-and-queue-coexist
  (let [pubsub (ps/make)
        plain  (atom 0)
        q      (atom 0)]
    (ps/sub pubsub ["x"] (fn [_ _ _] (swap! plain inc)))
    (ps/sub pubsub ["x"] (fn [_ _ _] (swap! q inc)) {:queue "g"})
    (ps/sub pubsub ["x"] (fn [_ _ _] (swap! q inc)) {:queue "g"})
    (ps/pub pubsub ["x"] :m)
    (ps/pub pubsub ["x"] :m)
    (is (= 2 @plain))
    (is (= 2 @q))))

(deftest unsub-is-idempotent
  (let [pubsub (ps/make)
        got    (atom 0)
        unsub  (ps/sub pubsub ["foo"] (fn [_ _ _] (swap! got inc)))]
    (ps/pub pubsub ["foo"] :m)
    (unsub)
    (unsub)
    (ps/pub pubsub ["foo"] :m)
    (is (= 1 @got))))

(deftest same-handler-twice-delivers-twice
  (let [pubsub  (ps/make)
        got     (atom 0)
        h       (fn [_ _ _] (swap! got inc))
        unsub-a (ps/sub pubsub ["foo"] h)
        unsub-b (ps/sub pubsub ["foo"] h)]
    (ps/pub pubsub ["foo"] :m)
    (is (= 2 @got) "two subs with the same handler → two deliveries")
    (unsub-a)
    (ps/pub pubsub ["foo"] :m)
    (is (= 3 @got))
    (unsub-b)
    (ps/pub pubsub ["foo"] :m)
    (is (= 3 @got))))

(deftest handler-throwing-does-not-break-others
  (let [pubsub (ps/make)
        got    (atom 0)]
    (ps/sub pubsub ["foo"] (fn [_ _ _] (throw (ex-info "boom" {}))))
    (ps/sub pubsub ["foo"] (fn [_ _ _] (swap! got inc)))
    (binding [*err* (java.io.StringWriter.)]
      (ps/pub pubsub ["foo"] :m))
    (is (= 1 @got))))

(deftest handler-receives-match-result
  (let [pubsub   (ps/make)
        received (atom [])]
    (ps/sub pubsub ["foo" :*] (fn [s m r] (swap! received conj [:a s m r])))
    (ps/sub pubsub ["foo" :*] (fn [s m r] (swap! received conj [:b s m r])))
    (ps/pub pubsub ["foo" "bar"] :m)
    (let [[[_ _ _ r1] [_ _ _ r2]] @received]
      (is (identical? r1 r2)
          "both handlers see the same match-result instance — computed once per pub")
      (is (= 2 (count (:plain r1))))
      (is (contains? r1 :plain))
      (is (contains? r1 :groups)))))

(deftest handler-match-result-shows-full-fan-out
  (let [pubsub (ps/make)
        seen   (atom nil)]
    (ps/sub pubsub ["work"] (fn [_ _ _] nil) {:queue "g"})
    (ps/sub pubsub ["work"] (fn [_ _ _] nil) {:queue "g"})
    (ps/sub pubsub ["work"] (fn [_ _ r] (reset! seen r)))
    (ps/pub pubsub ["work"] :m)
    (is (= 2 (count (get-in @seen [:groups "g"])))
        ":groups shows all queue members even though only one fired")
    (is (= 1 (count (:plain @seen)))
        ":plain contains the observer handler's own sub")))

(deftest pub-with-no-matches-is-safe
  (let [pubsub (ps/make)]
    (is (nil? (ps/pub pubsub ["unheard" "subject"] :m))
        "no subscribers → no crash, nil return")
    (ps/sub pubsub ["foo"] (fn [_ _ _] nil))
    (is (nil? (ps/pub pubsub ["bar"] :m))
        "sub exists but doesn't match → still safe")))

(deftest sub-chan-basic
  (let [pubsub     (ps/make)
        [ch stop!] (ps/sub-chan pubsub ["foo" :*] 10)]
    (ps/pub pubsub ["foo" "a"] :one)
    (ps/pub pubsub ["foo" "b"] :two)
    (is (= [["foo" "a"] :one] (async/<!! ch)))
    (is (= [["foo" "b"] :two] (async/<!! ch)))
    (stop!)
    (is (nil? (async/<!! ch)) "channel closes after stop!")))

(deftest sub-chan-queue-group
  (let [pubsub (ps/make)
        [a _]  (ps/sub-chan pubsub ["work"] 10 {:queue "g"})
        [b _]  (ps/sub-chan pubsub ["work"] 10 {:queue "g"})]
    (dotimes [_ 100] (ps/pub pubsub ["work"] :task))
    (Thread/sleep 50)  ; let async put! drain
    (let [drain (fn [ch]
                  (loop [n 0]
                    (if-let [_ (async/poll! ch)]
                      (recur (inc n))
                      n)))
          na (drain a)
          nb (drain b)]
      (is (= 100 (+ na nb)))
      (is (pos? na))
      (is (pos? nb)))))
