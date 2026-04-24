(ns toolkit.sublist-test
  (:require [clojure.test :refer [are deftest is testing]]
            [toolkit.stree :as stree]
            [toolkit.sublist :as sl])
  (:import [clojure.lang ExceptionInfo]))

(defn- reason [f]
  (try (f) nil (catch ExceptionInfo e (:reason (ex-data e)))))

(deftest match-table
  (let [s (sl/make)]
    (sl/insert! s ["foo" "bar" "baz"] :exact)
    (sl/insert! s ["foo" :*    "baz"] :pwc-mid)
    (sl/insert! s ["foo" "bar" :*]    :pwc-end)
    (sl/insert! s ["foo" :>]          :fwc)
    (sl/insert! s [:>]                :catchall)
    (sl/insert! s ["foo" :*    "qux"] :miss)

    (testing "foo.bar.baz — exact hits plus all wildcards"
      (is (= #{:exact :pwc-mid :pwc-end :fwc :catchall}
             (:plain (sl/match s ["foo" "bar" "baz"])))))

    (testing "foo.a.b.c — only tail wildcards"
      (is (= #{:fwc :catchall}
             (:plain (sl/match s ["foo" "a" "b" "c"])))))

    (testing "foo — too short for multi-token patterns, fwc needs >=1 tail"
      (is (= #{:catchall} (:plain (sl/match s ["foo"])))))

    (testing "foo.bar.different — partial wildcard at tail does match"
      (is (= #{:pwc-end :fwc :catchall}
             (:plain (sl/match s ["foo" "bar" "different"])))))

    (testing "foo.bar — too short for three-token patterns"
      (is (= #{:fwc :catchall} (:plain (sl/match s ["foo" "bar"])))))

    (testing "foo.bar.baz.qux — too long for foo.*.baz"
      (is (= #{:fwc :catchall} (:plain (sl/match s ["foo" "bar" "baz" "qux"])))))))

(deftest validation-rejection-reasons
  (let [s (sl/make)]
    (testing "pattern validation (insert)"
      (are [pattern expected] (= expected (reason #(sl/insert! s pattern :v)))
        []              :empty-subject
        nil             :empty-subject
        "foo"           :empty-subject    ; not a vector
        ["foo" "" "b"]  :empty-token
        ["" "foo"]      :empty-token
        ["foo" ""]      :empty-token
        [:> "foo"]      :fwc-not-final
        ["foo" :> "b"]  :fwc-not-final
        ["foo" 42]      :bad-token        ; non-string non-wildcard
        ["foo" :bad]    :bad-token
        ["foo" "b ar"]  :whitespace-in-token
        ["foo" "b\tar"] :whitespace-in-token))

    (testing "subject validation (match) additionally rejects wildcards"
      (are [subject expected] (= expected (reason #(sl/match s subject)))
        ["foo" :*]     :wildcard-in-subject
        ["foo" :>]     :wildcard-in-subject
        ["foo" "" "bar"] :empty-token))

    (testing "literal `*` / `>` strings are now valid tokens"
      (is (true? (sl/valid-subject? ["foo" "*" "bar"])))
      (is (true? (sl/valid-subject? ["foo" ">" "bar"])))
      (is (true? (sl/valid-pattern? ["foo" "*" "bar"]))))

    (testing "valid-pattern? / valid-subject? are non-throwing predicates"
      (is (true?  (sl/valid-pattern? ["foo" "bar" :*])))
      (is (true?  (sl/valid-pattern? ["foo" :>])))
      (is (false? (sl/valid-pattern? ["foo" :> "bar"])))
      (is (true?  (sl/valid-subject? ["foo" "bar" "baz"])))
      (is (false? (sl/valid-subject? ["foo" :*]))))))

(deftest prune-to-empty
  (let [s          (sl/make)
        fresh      @(sl/make)
        operations [[["a" "b" "c"] :x   nil]
                    [["a" :*  "c"] :y   "g"]
                    [["a" :>]      :z   nil]
                    [[:>]          :w   nil]
                    [["a" "b" "c"] :dup "g"]]]
    (doseq [[subj v q] operations]
      (sl/insert! s subj v (when q {:queue q})))
    (is (= (count operations) (sl/count-subs s))
        "count tracks every insert")
    (doseq [[subj v q] (shuffle operations)]
      (sl/remove! s subj v (when q {:queue q})))
    (is (zero? (sl/count-subs s))
        "count returns to zero after all removes")
    (is (= fresh @s)
        "after removing every inserted sub the trie should structurally match a fresh one")))

(deftest idempotent-insert
  (let [s (sl/make)]
    (sl/insert! s ["foo" "bar"] :x)
    (sl/insert! s ["foo" "bar"] :x)
    (is (= #{:x} (:plain (sl/match s ["foo" "bar"]))))
    (sl/remove! s ["foo" "bar"] :x)
    (is (= #{} (:plain (sl/match s ["foo" "bar"]))))))

(deftest absent-remove-is-noop
  (let [s (sl/make)]
    (is (= {:removed? false} (sl/remove! s ["foo" "bar"] :x)))
    (sl/insert! s ["foo" "bar"] :x)
    (is (= {:removed? true}  (sl/remove! s ["foo" "bar"] :x)))
    (is (= {:removed? false} (sl/remove! s ["foo" "bar"] :x)))))

(deftest queue-group-dedup-and-ordering
  (let [s (sl/make)]
    (sl/insert! s ["work"] :a {:queue "g"})
    (sl/insert! s ["work"] :b {:queue "g"})
    (sl/insert! s ["work"] :a {:queue "g"})
    (sl/insert! s ["work"] :a {:queue "h"})
    (let [r (sl/match s ["work"])]
      (is (= [:a :b] (get-in r [:groups "g"]))
          "dedup holds; insertion order preserved in the vector")
      (is (= [:a] (get-in r [:groups "h"]))
          "same value in a different queue is independent")
      (is (= 3 (sl/count-subs s))))))

(deftest queue-group-basics
  (let [s (sl/make)]
    (sl/insert! s ["work" "task"] :a {:queue "g"})
    (sl/insert! s ["work" "task"] :b {:queue "g"})
    (let [r (sl/match s ["work" "task"])]
      (is (= #{} (:plain r)))
      (is (= {"g" #{:a :b}} (update-vals (:groups r) set))
          "group members are stored as vectors; compare as sets"))))

(deftest mixed-plain-and-queue
  (let [s (sl/make)]
    (sl/insert! s ["x" "y"] :observer)
    (sl/insert! s ["x" "y"] :a {:queue "g"})
    (sl/insert! s ["x" "y"] :b {:queue "g"})
    (let [r (sl/match s ["x" "y"])]
      (is (= #{:observer} (:plain r)))
      (is (= {"g" #{:a :b}} (update-vals (:groups r) set))))))

(deftest fwc-and-queue
  (let [s (sl/make)]
    (sl/insert! s ["foo" :>] :a {:queue "g"})
    (sl/insert! s ["foo" :>] :b {:queue "g"})
    (let [r (sl/match s ["foo" "bar" "baz"])]
      (is (= {"g" #{:a :b}} (update-vals (:groups r) set))))))

(deftest absent-remove-preserves-identity
  (testing "no-op remove on an empty tree is structurally identity-preserving"
    (let [s      (sl/make)
          before @s]
      (is (= {:removed? false} (sl/remove! s ["foo" "bar"] :x)))
      (is (identical? before @s))))

  (testing "no-op remove of a different value at the same subject"
    (let [s (sl/make)]
      (sl/insert! s ["foo" "bar"] :x)
      (let [before @s]
        (is (= {:removed? false} (sl/remove! s ["foo" "bar"] :y)))
        (is (identical? before @s)))))

  (testing "plain/queue mismatch is a no-op — same subject/value, different queue dimension"
    (let [s (sl/make)]
      (sl/insert! s ["work"] :x)
      (let [before @s]
        (is (= {:removed? false} (sl/remove! s ["work"] :x {:queue "g"})))
        (is (identical? before @s)))))

  (testing "diverging path through wildcard/literal is a no-op"
    (let [s (sl/make)]
      (sl/insert! s ["a" "b"] :x)
      (let [before @s]
        (is (= {:removed? false} (sl/remove! s ["a" :*] :x)))
        (is (identical? before @s)
            "removing a.*:x when only a.b:x is registered must not perturb the tree")
        (is (= {:removed? false} (sl/remove! s ["c" "d"] :x)))
        (is (identical? before @s)
            "entirely absent path — no perturbation"))))

  (testing "a real removal changes identity"
    (let [s (sl/make)]
      (sl/insert! s ["foo"] :x)
      (let [before @s]
        (is (= {:removed? true} (sl/remove! s ["foo"] :x)))
        (is (not (identical? before @s))
            "present sub → tree structure must change")))))

(deftest has-interest-and-count
  (let [s (sl/make)]
    (is (false? (sl/has-interest? s ["foo" "bar"])))
    (is (zero?  (sl/count-subs s)))

    (sl/insert! s ["foo" :*] :a)
    (sl/insert! s ["foo" :>] :b)
    (sl/insert! s ["work"]   :x {:queue "g"})
    (sl/insert! s ["work"]   :y {:queue "g"})

    (is (true?  (sl/has-interest? s ["foo" "bar"])))
    (is (true?  (sl/has-interest? s ["foo" "bar" "baz"]))
        "foo.> matches multi-token tails")
    (is (false? (sl/has-interest? s ["other"])))
    (is (true?  (sl/has-interest? s ["work"]))
        "queue-only interest still counts")
    (is (= 4 (sl/count-subs s)))
    (sl/remove! s ["foo" :*] :a)
    (is (= 3 (sl/count-subs s))))

  (testing "prefix without a terminal sub is not interest"
    (let [s (sl/make)]
      (sl/insert! s ["deep" "nested" "subject"] :v)
      (is (false? (sl/has-interest? s ["deep"])))
      (is (false? (sl/has-interest? s ["deep" "nested"])))
      (is (true?  (sl/has-interest? s ["deep" "nested" "subject"])))))

  (testing "dedup: same (subject, value, queue) inserted twice counts once"
    (let [s (sl/make)]
      (sl/insert! s ["x"] :v)
      (sl/insert! s ["x"] :v)
      (is (= 1 (sl/count-subs s)))
      (sl/insert! s ["x"] :v {:queue "g"})
      (sl/insert! s ["x"] :v {:queue "g"})
      (is (= 2 (sl/count-subs s))
          "plain and queued versions of the same (subject, value) are independent"))))

(deftest reverse-match-basics
  (let [s (sl/make)]
    (sl/insert! s ["foo" "bar"]       :a)
    (sl/insert! s ["foo" "baz"]       :b)
    (sl/insert! s ["foo" "bar" "baz"] :c)
    (sl/insert! s ["other" "x"]       :d)

    (testing "foo.* returns only 2-token foo.x subs"
      (is (= #{:a :b} (:plain (sl/reverse-match s ["foo" :*])))))

    (testing "foo.> returns all foo-prefixed subs"
      (is (= #{:a :b :c} (:plain (sl/reverse-match s ["foo" :>])))))

    (testing "> returns every sub"
      (is (= #{:a :b :c :d} (:plain (sl/reverse-match s [:>])))))

    (testing "literal query degenerates to exact match"
      (is (= #{:a} (:plain (sl/reverse-match s ["foo" "bar"])))))))

(deftest reverse-match-queue-groups
  (let [s (sl/make)]
    (sl/insert! s ["foo" "bar"] :a {:queue "g"})
    (sl/insert! s ["foo" "baz"] :b {:queue "g"})
    (sl/insert! s ["foo" "bar"] :c {:queue "h"})
    (let [r (sl/reverse-match s ["foo" :*])]
      (is (= #{} (:plain r)))
      (is (= {"g" #{:a :b} "h" #{:c}}
             (update-vals (:groups r) set))
          "queue-group structure is preserved in reverse-match output"))))

(deftest reverse-match-wildcard-at-middle
  (let [s (sl/make)]
    (sl/insert! s ["a" "b" "c"] :1)
    (sl/insert! s ["a" "x" "c"] :2)
    (sl/insert! s ["a" "b" "d"] :3)
    (sl/insert! s ["a" "x" "d"] :4)
    (is (= #{:1 :2} (:plain (sl/reverse-match s ["a" :* "c"])))
        "wildcard at middle position matches any second token with `c` tail")
    (is (= #{:1 :3} (:plain (sl/reverse-match s ["a" "b" :*])))
        "wildcard at end position matches any third token")
    (is (= #{:1 :2 :3 :4} (:plain (sl/reverse-match s ["a" :* :*])))
        "two wildcards in query match any 3-token a-prefix")))

(deftest reverse-match-empty-cases
  (let [s (sl/make)]
    (is (= {:plain #{} :groups {}} (sl/reverse-match s ["foo" :*]))
        "empty sublist returns empty result")
    (sl/insert! s ["x" "y"] :v)
    (is (= {:plain #{} :groups {}} (sl/reverse-match s ["a" "b"]))
        "non-matching literal query returns empty result")
    (is (= {:plain #{} :groups {}} (sl/reverse-match s ["a" :>]))
        "non-matching wildcard query returns empty result")))

(deftest subject-matches-filter-basics
  (are [subject filter expected]
       (= expected (sl/subject-matches-filter? subject filter))
    ["foo" "bar"]       ["foo" :*]          true
    ["foo" "bar"]       ["foo" "bar"]       true
    ["foo" "bar"]       ["foo" "baz"]       false
    ["foo" "bar" "baz"] ["foo" :>]          true
    ["foo" "bar"]       [:>]                true
    ["foo" :*]          ["foo" :*]          true
    ["foo" :*]          ["foo" "bar"]       false
    ["foo" :*]          ["foo" :>]          true
    ["foo" :>]          ["foo" :>]          true
    ["foo" :>]          ["foo" :*]          false
    ["foo" "bar"]       ["foo" "bar" "baz"] false))

(deftest subjects-collide-basics
  (are [a b expected] (= expected (sl/subjects-collide? a b))
    ["foo" "bar"]       ["foo" "bar"]       true
    ["foo" "bar"]       ["foo" "baz"]       false
    ["foo" "bar"]       ["foo" :*]          true
    ["foo" "bar"]       ["foo" :>]          true
    ["foo" :*]          [:* "bar"]          true
    ["foo" :*]          ["foo" :* "baz"]    false
    ["foo" :>]          ["foo" :* "baz"]    true
    ["foo" "bar" "baz"] ["foo" :>]          true
    ["foo" "bar"]       ["baz" :>]          false
    ["a" "b"]           ["a" "b" "c"]       false))

;; --- intersect-stree ---
;;
;; `intersect-stree` bridges to `toolkit.stree` which is still string-
;; based; the cb receives dot-joined strings. Tests keep those string
;; subjects for the stree inserts and the cb assertions.

(defn- collect-into
  "Test helper: returns a cb that accretes `[subject value]` pairs into
   the given atom in call order."
  [!acc]
  (fn [subj v] (swap! !acc conj [subj v])))

(deftest intersect-stree-basics
  (testing "empty sublist — no callbacks"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo.bar" :v1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [] @!acc))))

  (testing "empty stree — no callbacks"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (sl/insert! sl ["foo" "bar"] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [] @!acc))))

  (testing "literal sub matching a literal entry — one cb"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo.bar" :v1)
      (sl/insert! sl ["foo" "bar"] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [["foo.bar" :v1]] @!acc))))

  (testing "literal sub not matching any entry — no cb"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo.bar" :v1)
      (sl/insert! sl ["foo" "baz"] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [] @!acc))))

  (testing "pwc sub matches a token-width class"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "a.b" :v1)
      (stree/insert! st "x.b" :v2)
      (stree/insert! st "y.c" :v3)
      (sl/insert! sl [:* "b"] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= #{["a.b" :v1] ["x.b" :v2]} (set @!acc)))))

  (testing "fwc sub matches every tail under its prefix"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo.a" :v1)
      (stree/insert! st "foo.b.c" :v2)
      (stree/insert! st "bar.a" :v3)
      (sl/insert! sl ["foo" :>] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= #{["foo.a" :v1] ["foo.b.c" :v2]} (set @!acc))))))

(deftest intersect-stree-dedup
  (testing "overlapping subs don't double-fire"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "a.b" :v1)
      (sl/insert! sl [:* "b"] :s1)
      (sl/insert! sl ["a" "b"] :s2)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [["a.b" :v1]] @!acc)
          "both subs would match a.b; cb must fire exactly once")))

  (testing "three-way overlap — fwc + pwc + literal"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo.bar" :v1)
      (sl/insert! sl ["foo" :>]    :s1)
      (sl/insert! sl ["foo" :*]    :s2)
      (sl/insert! sl ["foo" "bar"] :s3)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [["foo.bar" :v1]] @!acc)))))

(deftest intersect-stree-go-bug-regressions
  (testing "Go's `done` short-circuit would miss deep literal subs"
    ;; Counter-example: sublist has `a.*` (direct pwc interest) and
    ;; `a.foo.bar` (deep literal). Stree has `a.foo.bar` (3-token).
    ;; Go's `done=true` after match(a.*) skips the literal descent, so
    ;; a.foo.bar's interest is never processed.
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "a.foo.bar" :v1)
      (sl/insert! sl ["a" :*] :s1)
      (sl/insert! sl ["a" "foo" "bar"] :s2)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [["a.foo.bar" :v1]] @!acc)
          "must fire for a.foo.bar even though a.* doesn't match it")))

  (testing "Go's literal-sibling continue would miss unrelated literals"
    ;; Counter-example: sublist has `*.b` (pwc with one deep interest)
    ;; and `a.c` (unrelated literal sibling). Stree has `a.c`. Go skips
    ;; the literal-a descent because pwc.next has nodes, missing `a.c`.
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "a.c" :v1)
      (sl/insert! sl [:* "b"] :s1)
      (sl/insert! sl ["a" "c"] :s2)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= [["a.c" :v1]] @!acc)
          "must fire for a.c — pwc subtree's interest doesn't cover it"))))

(deftest intersect-stree-fwc-prune
  (testing "sub `>` fires once per entry"
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "foo"     :v1)
      (stree/insert! st "foo.bar" :v2)
      (stree/insert! st "a.b.c"   :v3)
      (sl/insert! sl [:>] :s1)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= #{["foo" :v1] ["foo.bar" :v2] ["a.b.c" :v3]} (set @!acc)))
      (is (= 3 (count @!acc)) "exactly one cb per entry"))))

(deftest intersect-stree-mixed-literal-and-wildcard
  (testing "subject shared by a direct-literal sub and a deeper fwc"
    ;; Sub `x` (literal 1-token) AND sub `x.>` (deeper fwc). Both sit
    ;; at the same sublist node (children[x] with both :psubs and :fwc).
    ;; Stree has `x` (matched by literal sub) and `x.a` (matched by fwc).
    (let [st (stree/make)
          sl (sl/make)
          !acc (atom [])]
      (stree/insert! st "x"   :v1)
      (stree/insert! st "x.a" :v2)
      (sl/insert! sl ["x"]     :s1)
      (sl/insert! sl ["x" :>]  :s2)
      (sl/intersect-stree st sl (collect-into !acc))
      (is (= #{["x" :v1] ["x.a" :v2]} (set @!acc))
          "literal-x sub processed by lookup; fwc subsumes the rest"))))

(deftest match-cache
  (testing "repeated match populates cache and returns identical result"
    (let [s (sl/make)]
      (sl/insert! s ["foo" :>] :a)
      (sl/insert! s ["foo" "bar"] :b)
      (is (zero? (:cache-size (sl/stats s))))
      (let [r1 (sl/match s ["foo" "bar"])]
        (is (= 1 (:cache-size (sl/stats s))))
        (is (identical? r1 (sl/match s ["foo" "bar"]))
            "cache hit returns the same object"))))

  (testing "real insert clears cache"
    (let [s (sl/make)]
      (sl/insert! s ["foo" "bar"] :a)
      (sl/match s ["foo" "bar"])
      (is (= 1 (:cache-size (sl/stats s))))
      (sl/insert! s ["foo" "baz"] :c)
      (is (zero? (:cache-size (sl/stats s))))))

  (testing "real remove clears cache"
    (let [s (sl/make)]
      (sl/insert! s ["foo" "bar"] :a)
      (sl/match s ["foo" "bar"])
      (is (= 1 (:cache-size (sl/stats s))))
      (sl/remove! s ["foo" "bar"] :a)
      (is (zero? (:cache-size (sl/stats s))))))

  (testing "no-op write preserves cache"
    (let [s (sl/make)]
      (sl/insert! s ["foo" "bar"] :a)
      (sl/match s ["foo" "bar"])
      (let [before @s]
        (sl/insert! s ["foo" "bar"] :a)          ; duplicate
        (is (identical? before @s))
        (sl/remove! s ["nope" "nope"] :x)        ; absent
        (is (identical? before @s))
        (is (= 1 (:cache-size (sl/stats s)))))))

  (testing "cached result stays correct across mutations"
    (let [s (sl/make)]
      (sl/insert! s ["a"] :x)
      (is (= #{:x} (:plain (sl/match s ["a"]))))
      (sl/insert! s ["a"] :y)
      (is (= #{:x :y} (:plain (sl/match s ["a"]))))
      (sl/remove! s ["a"] :x)
      (is (= #{:y} (:plain (sl/match s ["a"])))))))

(deftest concurrency-smoke
  (let [s        (sl/make)
        subjects (vec (for [a (range 5) b (range 5)] ["s" (str a) (str b)]))
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
