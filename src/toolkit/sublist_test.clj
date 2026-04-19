(ns toolkit.sublist-test
  (:require [clojure.test :refer [are deftest is testing]]
            [toolkit.sublist :as sl])
  (:import [clojure.lang ExceptionInfo]))

(defn- reason [f]
  (try (f) nil (catch ExceptionInfo e (:reason (ex-data e)))))

(deftest match-table
  (let [s (sl/make)]
    (sl/insert! s "foo.bar.baz" :exact)
    (sl/insert! s "foo.*.baz"   :pwc-mid)
    (sl/insert! s "foo.bar.*"   :pwc-end)
    (sl/insert! s "foo.>"       :fwc)
    (sl/insert! s ">"           :catchall)
    (sl/insert! s "foo.*.qux"   :miss)

    (testing "foo.bar.baz — exact hits plus all wildcards"
      (is (= #{:exact :pwc-mid :pwc-end :fwc :catchall}
             (:plain (sl/match s "foo.bar.baz")))))

    (testing "foo.a.b.c — only tail wildcards"
      (is (= #{:fwc :catchall}
             (:plain (sl/match s "foo.a.b.c")))))

    (testing "foo — too short for multi-token patterns, fwc needs >=1 tail"
      (is (= #{:catchall} (:plain (sl/match s "foo")))))

    (testing "foo.bar.different — partial wildcard at tail does match"
      (is (= #{:pwc-end :fwc :catchall}
             (:plain (sl/match s "foo.bar.different")))))

    (testing "foo.bar — too short for three-token patterns"
      (is (= #{:fwc :catchall} (:plain (sl/match s "foo.bar")))))

    (testing "foo.bar.baz.qux — too long for foo.*.baz"
      (is (= #{:fwc :catchall} (:plain (sl/match s "foo.bar.baz.qux")))))))

(deftest validation-rejection-reasons
  (let [s (sl/make)]
    (testing "pattern validation (insert)"
      (are [pattern expected] (= expected (reason #(sl/insert! s pattern :v)))
        ""          :empty-subject
        nil         :empty-subject
        "foo..b"    :empty-token
        ".foo"      :empty-token
        "foo."      :empty-token
        ">.foo"     :fwc-not-final
        "foo.>.b"   :fwc-not-final
        "foo*"      :bad-wildcard
        "foo.*bar"  :bad-wildcard
        "foo.>x"    :bad-wildcard
        "foo.b ar"  :whitespace-in-token
        "foo.b\tar" :whitespace-in-token))

    (testing "subject validation (match) additionally rejects wildcards"
      (are [subject expected] (= expected (reason #(sl/match s subject)))
        "foo.*"    :wildcard-in-subject
        "foo.>"    :wildcard-in-subject
        "foo..bar" :empty-token))

    (testing "valid-pattern? / valid-subject? are non-throwing predicates"
      (is (true?  (sl/valid-pattern? "foo.bar.*")))
      (is (true?  (sl/valid-pattern? "foo.>")))
      (is (false? (sl/valid-pattern? "foo.>.bar")))
      (is (true?  (sl/valid-subject? "foo.bar.baz")))
      (is (false? (sl/valid-subject? "foo.*"))))))

(deftest prune-to-empty
  (let [s          (sl/make)
        fresh      @(sl/make)
        operations [["a.b.c" :x   nil]
                    ["a.*.c" :y   "g"]
                    ["a.>"   :z   nil]
                    [">"     :w   nil]
                    ["a.b.c" :dup "g"]]]
    (doseq [[subj v q] operations]
      (sl/insert! s subj v (when q {:queue q})))
    (doseq [[subj v q] (shuffle operations)]
      (sl/remove! s subj v (when q {:queue q})))
    (is (= fresh @s)
        "after removing every inserted sub the trie should structurally match a fresh one")))

(deftest idempotent-insert
  (let [s (sl/make)]
    (sl/insert! s "foo.bar" :x)
    (sl/insert! s "foo.bar" :x)
    (is (= #{:x} (:plain (sl/match s "foo.bar"))))
    (sl/remove! s "foo.bar" :x)
    (is (= #{} (:plain (sl/match s "foo.bar"))))))

(deftest absent-remove-is-noop
  (let [s (sl/make)]
    (is (= {:removed? false} (sl/remove! s "foo.bar" :x)))
    (sl/insert! s "foo.bar" :x)
    (is (= {:removed? true}  (sl/remove! s "foo.bar" :x)))
    (is (= {:removed? false} (sl/remove! s "foo.bar" :x)))))

(deftest queue-group-basics
  (let [s (sl/make)]
    (sl/insert! s "work.task" :a {:queue "g"})
    (sl/insert! s "work.task" :b {:queue "g"})
    (let [r (sl/match s "work.task")]
      (is (= #{} (:plain r)))
      (is (= {"g" #{:a :b}} (:groups r)))
      (let [picked (sl/pick-one r)]
        (is (= 1 (count picked)))
        (is (contains? #{:a :b} (first picked)))))))

(deftest mixed-plain-and-queue
  (let [s (sl/make)]
    (sl/insert! s "x.y" :observer)
    (sl/insert! s "x.y" :a {:queue "g"})
    (sl/insert! s "x.y" :b {:queue "g"})
    (let [r      (sl/match s "x.y")
          picked (sl/pick-one r)]
      (is (= #{:observer} (:plain r)))
      (is (= {"g" #{:a :b}} (:groups r)))
      (is (contains? picked :observer))
      (is (= 2 (count picked))))))

(deftest fwc-and-queue
  (let [s (sl/make)]
    (sl/insert! s "foo.>" :a {:queue "g"})
    (sl/insert! s "foo.>" :b {:queue "g"})
    (let [r (sl/match s "foo.bar.baz")]
      (is (= {"g" #{:a :b}} (:groups r))))))

(deftest concurrency-smoke
  (let [s        (sl/make)
        subjects (vec (for [a (range 5) b (range 5)] (str "s." a "." b)))
        ops      (doall
                   (repeatedly 500
                     #(let [subj (rand-nth subjects)
                            v    (rand-int 50)
                            op   (rand-nth [:ins :rm :match])]
                        [op subj v])))]
    (is (= 500
           (count
             (doall
               (pmap (fn [[op subj v]]
                       (case op
                         :ins   (sl/insert! s subj v)
                         :rm    (sl/remove! s subj v)
                         :match (sl/match s subj))
                       :ok)
                     ops)))))))
