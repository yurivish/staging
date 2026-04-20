(ns toolkit.singleflight-test
  (:require [clojure.test :refer [deftest is testing]]
            [toolkit.singleflight :as sf]))

;; Rendezvous strategy
;; -------------------
;; Singleflight's guarantee is "exactly one fn invocation per batch of
;; concurrent callers." To test that deterministically we need to pin a leader
;; inside fn while every follower has observably registered on the shared
;; `delay` — otherwise a fast leader might finish and dissoc its entry before
;; the followers even arrive, producing a false "dedup" (it was really serial).
;;
;; Incrementing a counter before calling do! only proves the thread started;
;; it says nothing about reaching swap-vals!. Instead we look at Thread state:
;; once a follower calls @delay during the leader's compute, the JVM parks it
;; on the delay's intrinsic monitor (BLOCKED), and the leader — stuck on a
;; promise deref inside fn — sits in WAITING. `await-all-parked` spins until
;; every test thread is in one of those states, giving a deterministic,
;; no-timeout rendezvous.

;; --- rendezvous helpers ---------------------------------------------------

(defn- parked?
  "True once the thread has parked on a monitor/lock — either BLOCKED on the
   delay's intrinsic monitor (follower) or WAITING on a latch (leader)."
  [^Thread t]
  (contains? #{Thread$State/WAITING
               Thread$State/TIMED_WAITING
               Thread$State/BLOCKED}
             (.getState t)))

(defn- await-all-parked [threads]
  (while (not-every? parked? threads) (Thread/yield)))

(defn- spin-until [pred]
  (while (not (pred)) (Thread/yield)))

(defn- run-thread
  "Spawn a thread running f. Returns {:thread t :result promise} where the
   promise receives {:val v} or {:ex e}."
  [f]
  (let [p (promise)
        t (Thread. ^Runnable
                   (fn []
                     (try (deliver p {:val (f)})
                          (catch Throwable e (deliver p {:ex e})))))]
    (.start t)
    {:thread t :result p}))

(defn- await-result [{:keys [^Thread thread result]}]
  (.join thread)
  @result)

(defn- gated-fn
  "A function that increments :calls, blocks on :proceed, then returns result.
   :release releases the gate."
  [result]
  (let [calls   (atom 0)
        proceed (promise)]
    {:f       (fn [] (swap! calls inc) @proceed result)
     :calls   calls
     :release #(deliver proceed :go)}))

;; --- core dedup -----------------------------------------------------------

(deftest dedups-concurrent-calls
  (doseq [n [2 8 64]]
    (testing (str "n=" n)
      (let [g        (sf/group)
            gate     (gated-fn :result)
            threads  (doall (for [_ (range n)]
                              (run-thread #(sf/do! g "k" (:f gate)))))]
        ;; two-phase rendezvous: first the leader enters fn (calls=1), then
        ;; every other thread parks on @delay. Only after both do we assert.
        (spin-until #(= 1 @(:calls gate)))
        (await-all-parked (map :thread threads))
        (is (= 1 @(:calls gate))
            "only one leader entered fn while followers were parked")
        ((:release gate))
        (doseq [t threads]
          (is (= {:val :result} (await-result t))))
        (is (= 1 @(:calls gate))
            "no extra invocations after release")
        (is (nil? (get @g "k"))
            "entry removed after call completes")))))

(deftest sequential-calls-re-evaluate
  (let [g     (sf/group)
        calls (atom 0)
        f     (fn [] (swap! calls inc) :x)]
    (is (= :x (sf/do! g "k" f)))
    (is (= :x (sf/do! g "k" f)))
    (is (= 2 @calls) "each sequential call ran fn (no caching across calls)")))

(deftest different-keys-do-not-share
  (let [g        (sf/group)
        n        8
        gates    (mapv (fn [i] (gated-fn [:result i])) (range n))
        threads  (mapv (fn [i] (run-thread #(sf/do! g (str "key-" i) (:f (gates i)))))
                       (range n))]
    (doseq [i (range n)]
      (spin-until #(= 1 @(:calls (gates i)))))
    (await-all-parked (map :thread threads))
    (doseq [i (range n)]
      (is (= 1 @(:calls (gates i)))))
    (doseq [gate gates] ((:release gate)))
    (doseq [i (range n)]
      (is (= {:val [:result i]} (await-result (threads i)))))))

;; --- exceptional paths ----------------------------------------------------

(deftest exception-propagates-to-all-waiters
  (let [g        (sf/group)
        calls    (atom 0)
        proceed  (promise)
        boom     (ex-info "boom" {:marker :x})
        f        (fn [] (swap! calls inc) @proceed (throw boom))
        n        4
        threads  (doall (for [_ (range n)]
                          (run-thread #(sf/do! g "k" f))))]
    (spin-until #(= 1 @calls))
    (await-all-parked (map :thread threads))
    (deliver proceed :go)
    ;; identical? (not just equality) — Delay caches the exception in its
    ;; `exception` field and rethrows the same instance on every deref, so
    ;; every waiter receives the exact same object the leader's fn threw.
    (doseq [t threads]
      (let [r (await-result t)]
        (is (identical? boom (:ex r))
            "every waiter saw the same exception instance")))
    (is (= 1 @calls) "fn ran once even under exception")
    (is (nil? (get @g "k")) "entry cleaned up after exceptional call")))

(deftest post-exception-next-call-runs-fresh
  (let [g     (sf/group)
        calls (atom 0)
        boomf (fn [] (swap! calls inc) (throw (ex-info "boom" {})))
        okf   (fn [] (swap! calls inc) :recovered)]
    (is (thrown? clojure.lang.ExceptionInfo (sf/do! g "k" boomf)))
    (is (= :recovered (sf/do! g "k" okf)))
    (is (= 2 @calls))))

;; --- forget mid-flight ----------------------------------------------------

(deftest forget-mid-flight-starts-fresh-execution
  (let [g  (sf/group)
        a  (gated-fn :r1)
        b  (gated-fn :r2)
        ta (run-thread #(sf/do! g "k" (:f a)))]
    (spin-until #(= 1 @(:calls a)))
    (await-all-parked [(:thread ta)])
    (sf/forget g "k")
    (is (nil? (get @g "k")) "forget cleared the map entry")
    (let [tb (run-thread #(sf/do! g "k" (:f b)))]
      (spin-until #(= 1 @(:calls b)))
      ;; C arrives after B installed its delay — should share with B.
      (let [tc (run-thread #(sf/do! g "k" (:f b)))]
        (await-all-parked [(:thread tb) (:thread tc)])
        (is (= 1 @(:calls a)) "leader A still only ran once")
        (is (= 1 @(:calls b)) "leader B ran once, C shared its delay")
        ((:release a))
        (is (= {:val :r1} (await-result ta)))
        ;; A's finally must NOT have wiped B's entry (identity guard).
        (is (some? (get @g "k")) "B's entry survives A's cleanup")
        ((:release b))
        (is (= {:val :r2} (await-result tb)))
        (is (= {:val :r2} (await-result tc)))
        (is (= 1 @(:calls b)) "C did not trigger a fresh invocation")))))

;; --- randomized stress ----------------------------------------------------

(deftest randomized-stress
  (let [k        4
        n        32
        m        500
        ks       (mapv #(str "key-" %) (range k))
        g        (sf/group)
        calls    (atom (zipmap ks (repeat 0)))
        barrier  (java.util.concurrent.CyclicBarrier. n)
        ;; tiny busy-work inside fn widens the delay-held window so followers
        ;; have a realistic chance to pile up behind the leader.
        work     #(reduce + 0 (range 2000))
        worker   (fn [seed]
                   (let [rng (java.util.Random. seed)
                         out (transient [])]
                     (.await barrier)
                     (dotimes [_ m]
                       (let [key   (ks (.nextInt rng k))
                             token (sf/do! g key
                                           (fn []
                                             (work)
                                             (let [c (get (swap! calls update key inc) key)]
                                               [key c])))]
                         (conj! out token)))
                     (persistent! out)))
        workers  (mapv #(future (worker %)) (range n))
        all-ops  (into [] cat (map deref workers))
        total    (count all-ops)]

    (is (= (* n m) total) "every op produced a token")

    (doseq [key ks]
      (let [ops-for-key (filterv #(= key (first %)) all-ops)
            unique      (into #{} ops-for-key)
            calls-k     (get @calls key)]
        (is (= calls-k (count unique))
            (str key ": distinct tokens == invocations"))
        (is (<= calls-k (count ops-for-key))
            (str key ": invocations ≤ ops"))
        (is (pos? calls-k) (str key ": got at least one invocation"))))

    (let [total-calls (apply + (vals @calls))]
      (is (< total-calls total)
          (str "dedup must actually happen: got "
               total-calls " invocations across " total " ops")))

    (is (empty? @g) "map drained after all workers finished")))
