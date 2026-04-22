(ns toolkit.datapotamus.proc-test
  (:require [clojure.core.async.flow :as flow]
            [clojure.test :refer [deftest is]]
            [toolkit.datapotamus.proc :as proc]))

(defn- msg [data]
  {:msg-id (random-uuid) :data-id (random-uuid)
   :run-id (random-uuid) :data data})

(deftest derive-carries-run-fresh-ids-new-data
  (let [p (msg 10) c (proc/derive p 9)]
    (is (= (:run-id p) (:run-id c)))
    (is (not= (:msg-id p) (:msg-id c)))
    (is (not= (:data-id p) (:data-id c)))
    (is (= 9 (:data c)))))

(deftest send-out-event-shape
  (let [p (msg 10) c (proc/derive p 9)
        e (proc/send-out-event :a c :parent-msg-ids [(:msg-id p)]
                                :to-step-id :b :provenance {:idx 0})]
    (is (= :send-out (:kind e)))
    (is (= :a (:step-id e)))
    (is (= :b (:to-step-id e)))
    (is (= [(:msg-id p)] (:parent-msg-ids e)))
    (is (= {:idx 0} (:provenance e)))))

(deftest recv-success-failure-merge-sink-shapes
  (let [m (msg nil)]
    (is (= :recv     (:kind (proc/recv-event :s m))))
    (is (= :success  (:kind (proc/success-event :s m))))
    (let [f (proc/failure-event :s m (ex-info "boom" {}))]
      (is (= :failure (:kind f)))
      (is (= "boom"   (get-in f [:error :message]))))
    (let [e (proc/merge-event :s m [(random-uuid) (random-uuid)])]
      (is (= :merge (:kind e)))
      (is (= 2 (count (:parent-msg-ids e)))))
    (is (= :sink-wrote
           (:kind (proc/sink-wrote-event :s m {:kind :sqlite :location "t:1"}))))))

(deftest step-proc-emits-recv-sendout-success
  (let [sfn (proc/step-proc :dec (fn [m] (update m :data dec)))
        _ (sfn) state (sfn {}) state (sfn state :resume)
        seed (msg 10)
        [_ out] (sfn state :in seed)
        evs (::flow/report out)
        child (first (:out out))]
    (is (= 9 (:data child)))
    (is (= (:run-id seed) (:run-id child)))
    (is (= [:recv :send-out :success] (mapv :kind evs)))
    (is (= [(:msg-id seed)] (:parent-msg-ids (second evs))))))

(deftest step-proc-rethrows-with-failure-event-attached
  (let [sfn (proc/step-proc :boom (fn [_] (throw (ex-info "no" {:a 1}))))]
    (sfn)
    (let [s (sfn {})]
      (try (sfn s :in (msg nil))
           (is false "should have thrown")
           (catch clojure.lang.ExceptionInfo ex
             (is (some? (::proc/failure-event (ex-data ex)))))))))
