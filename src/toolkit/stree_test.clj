(ns toolkit.stree-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [toolkit.stree :as st]))

;; --- helpers ---

(defn- collect
  "Collects (subject, value) pairs from a wildcard match."
  [tree filter]
  (let [acc (atom [])]
    (st/match tree filter (fn [s v] (swap! acc conj [s v])))
    @acc))

(defn- count-match [tree filter]
  (count (collect tree filter)))

(defn- leaf? [n] (contains? n :suffix))

;; --- basics / split ---

(deftest basics
  (let [t (st/make)]
    (is (zero? (st/size t)))
    (is (= [nil false] (st/insert! t "foo.bar.baz" 22)))
    (is (= 1 (st/size t)))
    (is (= [nil false] (st/lookup t "foo.bar.*")) "literal lookup rejects wildcards")
    (is (= [22 true] (st/lookup t "foo.bar.baz")))
    (is (= [22 true] (st/insert! t "foo.bar.baz" 33)) "update returns old value")
    (is (= 1 (st/size t)))
    (is (= [nil false] (st/insert! t "foo.bar" 22)) "splits into internal node")
    (is (= 2 (st/size t)))
    (is (= [22 true] (st/lookup t "foo.bar")))
    (is (= [33 true] (st/lookup t "foo.bar.baz")))))

(deftest node-prefix-mismatch
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 11)
    (st/insert! t "foo.bar.B" 22)
    (st/insert! t "foo.bar.C" 33)
    (let [or-root (:root @t)]
      (st/insert! t "foo.foo.A" 44)
      (is (not= or-root (:root @t)) "root changed from prefix split"))
    (is (= [11 true] (st/lookup t "foo.bar.A")))
    (is (= [22 true] (st/lookup t "foo.bar.B")))
    (is (= [33 true] (st/lookup t "foo.bar.C")))
    (is (= [44 true] (st/lookup t "foo.foo.A")))))

(deftest delete-single-leaf-clears-root
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 22)
    (is (= [22 true] (st/delete! t "foo.bar.A")))
    (is (nil? (:root @t)))
    (is (= [nil false] (st/delete! t "foo.bar.A")))
    (is (= [nil false] (st/lookup t "foo.foo.A")))))

(deftest delete-shrinks-and-absorbs-prefix
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 11)
    (st/insert! t "foo.bar.B" 22)
    (st/insert! t "foo.bar.C" 33)
    (is (= [33 true] (st/delete! t "foo.bar.C")))
    (is (= [22 true] (st/delete! t "foo.bar.B")))
    ;; Sole remaining child collapses into a leaf at the root.
    (is (leaf? (:root @t)))
    (is (= [11 true] (st/delete! t "foo.bar.A")))
    (is (nil? (:root @t)))))

(deftest nodes-and-paths-prefix-absorption
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 22)
    (st/insert! t "foo.bar.B" 22)
    (st/insert! t "foo.bar.C" 22)
    (st/insert! t "foo.bar"   22)
    (doseq [s ["foo.bar.A" "foo.bar.B" "foo.bar.C" "foo.bar"]]
      (is (= [22 true] (st/lookup t s))))
    ;; Deleting "foo.bar" should trigger shrink + prefix absorption in the
    ;; remaining internal node holding .A/.B/.C.
    (st/delete! t "foo.bar")
    (doseq [s ["foo.bar.A" "foo.bar.B" "foo.bar.C"]]
      (is (= [22 true] (st/lookup t s))))))

(deftest construction-shape
  ;; Replaces Go's TestSubjectTreeConstruction: we assert on our map
  ;; shape (not on adaptive node types, which we don't have).
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 1)
    (st/insert! t "foo.bar.B" 2)
    (st/insert! t "foo.bar.C" 3)
    (st/insert! t "foo.baz.A" 11)
    (st/insert! t "foo.baz.B" 22)
    (st/insert! t "foo.baz.C" 33)
    (st/insert! t "foo.bar"   42)

    (let [root (:root @t)]
      (is (not (leaf? root)))
      (is (= "foo.ba" (:prefix root)))
      (is (= 2 (count (:children root))))
      (let [r-branch (get (:children root) \r)
            z-branch (get (:children root) \z)]
        (is (= "r" (:prefix r-branch)))
        (is (= 2 (count (:children r-branch))))
        ;; The leaf for "foo.bar" lives under no-pivot (it ran out of
        ;; subject after the "r" prefix).
        (let [leaf-for-foo-bar (get (:children r-branch) \u007f)
              dot-branch       (get (:children r-branch) \.)]
          (is (leaf? leaf-for-foo-bar))
          (is (= "" (:suffix leaf-for-foo-bar)))
          (is (= 42 (:value leaf-for-foo-bar)))
          (is (= "." (:prefix dot-branch)))
          (is (= 3 (count (:children dot-branch)))))
        (is (= "z." (:prefix z-branch)))
        (is (= 3 (count (:children z-branch))))))

    ;; Deleting "foo.bar" should collapse r-branch to a node with "r."
    ;; prefix and three leaf children (A/B/C).
    (is (= [42 true] (st/delete! t "foo.bar")))
    (let [root (:root @t)
          r    (get (:children root) \r)]
      (is (= "r." (:prefix r)))
      (is (= 3 (count (:children r))))
      (doseq [k [\A \B \C]]
        (is (leaf? (get (:children r) k)))))))

;; --- match ---

(deftest match-leaf-only
  (let [t (st/make)]
    (st/insert! t "foo.bar.baz.A" 1)
    ;; pwc in all positions
    (is (= 1 (count-match t "foo.bar.*.A")))
    (is (= 1 (count-match t "foo.*.baz.A")))
    (is (= 1 (count-match t "foo.*.*.A")))
    (is (= 1 (count-match t "foo.*.*.*")))
    (is (= 1 (count-match t "*.*.*.*")))
    ;; fwc
    (is (= 1 (count-match t ">")))
    (is (= 1 (count-match t "foo.>")))
    (is (= 1 (count-match t "foo.*.>")))
    (is (= 1 (count-match t "foo.bar.>")))
    (is (= 1 (count-match t "foo.bar.*.>")))
    ;; partial subject shouldn't trigger on the 4-token leaf
    (is (= 0 (count-match t "foo.bar.baz")))))

(deftest match-nodes
  (let [t (st/make)]
    (st/insert! t "foo.bar.A" 1)
    (st/insert! t "foo.bar.B" 2)
    (st/insert! t "foo.bar.C" 3)
    (st/insert! t "foo.baz.A" 11)
    (st/insert! t "foo.baz.B" 22)
    (st/insert! t "foo.baz.C" 33)

    (testing "literals"
      (is (= 1 (count-match t "foo.bar.A")))
      (is (= 1 (count-match t "foo.baz.A")))
      (is (= 0 (count-match t "foo.bar"))))
    (testing "internal pwc"
      (is (= 2 (count-match t "foo.*.A"))))
    (testing "terminal pwc"
      (is (= 3 (count-match t "foo.bar.*")))
      (is (= 3 (count-match t "foo.baz.*"))))
    (testing "fwc"
      (is (= 6 (count-match t ">")))
      (is (= 6 (count-match t "foo.>")))
      (is (= 3 (count-match t "foo.bar.>")))
      (is (= 3 (count-match t "foo.baz.>"))))
    (testing "no false positives on prefix"
      (is (= 0 (count-match t "foo.ba"))))

    ;; Add "foo.bar" to stress the case where an internal node has a
    ;; terminal leaf hanging off its noPivot slot.
    (st/insert! t "foo.bar" 42)
    (is (= 1 (count-match t "foo.bar")))
    (is (= 2 (count-match t "foo.*.A")))
    (is (= 7 (count-match t ">")))
    (is (= 7 (count-match t "foo.>")))
    ;; Terminal `*` should NOT match `foo.bar` itself (it matches one
    ;; more token), but `foo.bar.*` still matches the three A/B/C.
    (is (= 3 (count-match t "foo.bar.*")))))

(deftest no-prefix-root
  (let [t (st/make)]
    (doseq [c "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
      (st/insert! t (str c) 22))
    (let [root (:root @t)]
      (is (= "" (:prefix root)))
      (is (= 26 (count (:children root)))))
    (is (= [22 true] (st/delete! t "B")))
    (is (= 25 (count (:children (:root @t)))))
    (is (= [22 true] (st/delete! t "Z")))
    (is (= 24 (count (:children (:root @t)))))))

(deftest partial-terminal-wildcard-bug-match
  ;; Regression: stree.go:417. Subjects with long common prefixes plus a
  ;; terminal PWC must not drop matches on the internal partial-match path.
  (let [t (st/make)]
    (st/insert! t "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-A" 5)
    (st/insert! t "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-B" 1)
    (st/insert! t "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.PROPERTY-C" 2)
    (is (= 3 (count-match t "STATE.GLOBAL.CELL1.7PDSGAALXNN000010.*")))))

(deftest match-subject-param
  (let [t (st/make)]
    (doseq [[s v] [["foo.bar.A" 1]   ["foo.bar.B" 2]   ["foo.bar.C" 3]
                   ["foo.baz.A" 11]  ["foo.baz.B" 22]  ["foo.baz.C" 33]
                   ["foo.bar"   42]]]
      (st/insert! t s v))
    (let [seen (atom {})]
      (st/match t ">" (fn [s v] (swap! seen assoc s v)))
      (is (= {"foo.bar.A" 1  "foo.bar.B" 2  "foo.bar.C" 3
              "foo.baz.A" 11 "foo.baz.B" 22 "foo.baz.C" 33
              "foo.bar"   42} @seen)))))

(deftest match-random-double-pwc
  ;; Large-ish match test (stree.go:454 used 10k; we use 2k for speed).
  (let [t (st/make)
        n 2000]
    (dotimes [i n]
      (st/insert! t (str "foo." (rand-int 20) "." i) 42))
    (is (= n (count-match t "foo.*.*")))
    (let [seen-2     (count-match t "*.2.*")
          verified-2 (atom 0)]
      (st/iter-ordered t (fn [s _v]
                           (when (= "2" (nth (str/split s #"\.") 1))
                             (swap! verified-2 inc))
                           true))
      (is (= seen-2 @verified-2)))))

(deftest iter-ordered-walk-and-early-stop
  (let [t (st/make)]
    (doseq [[s v] [["foo.bar.A" 1] ["foo.bar.B" 2] ["foo.bar.C" 3]
                   ["foo.baz.A" 11] ["foo.baz.B" 22] ["foo.baz.C" 33]
                   ["foo.bar" 42]]]
      (st/insert! t s v))
    (let [expected-order ["foo.bar" "foo.bar.A" "foo.bar.B" "foo.bar.C"
                          "foo.baz.A" "foo.baz.B" "foo.baz.C"]
          visited        (atom [])]
      (st/iter-ordered t (fn [s _v] (swap! visited conj s) true))
      (is (= expected-order @visited)))
    (let [n-visited (atom 0)]
      (st/iter-ordered t (fn [_s _v] (swap! n-visited inc) (< @n-visited 4)))
      (is (= 4 @n-visited) "early-stop on falsey return"))))

(deftest iter-fast-walks-everything
  (let [t   (st/make)
        exp {"foo.bar.A" 1  "foo.bar.B" 2  "foo.bar.C" 3
             "foo.baz.A" 11 "foo.baz.B" 22 "foo.baz.C" 33
             "foo.bar"   42}]
    (doseq [[s v] exp] (st/insert! t s v))
    (let [seen (atom {})]
      (st/iter-fast t (fn [s v] (swap! seen assoc s v) true))
      (is (= exp @seen)))
    (let [n (atom 0)]
      (st/iter-fast t (fn [_s _v] (swap! n inc) (< @n 4)))
      (is (= 4 @n)))))

(deftest iter-ordered-lex-across-subtrees
  ;; Verifies our simplified "sort children by key char" produces the
  ;; correct lex order across subtrees — a concern called out in the plan.
  (let [t (st/make)]
    (doseq [s ["foo.baz" "foo.baz.A" "foo.bar" "foo.bar.A"]]
      (st/insert! t s s))
    (let [visited (atom [])]
      (st/iter-ordered t (fn [s _v] (swap! visited conj s) true))
      (is (= ["foo.bar" "foo.bar.A" "foo.baz" "foo.baz.A"] @visited)))))

(deftest insert-same-pivot-bug
  ;; Regression: stree.go:587. Random-ish subjects whose splits
  ;; repeatedly collide on the same pivot after a common prefix.
  (let [t        (st/make)
        subjects ["0d00.2abbb82c1d.6e16.fa7f85470e.3e46"
                  "534b12.3486c17249.4dde0666"
                  "6f26aabd.920ee3.d4d3.5ffc69f6"
                  "8850.ade3b74c31.aa533f77.9f59.a4bd8415.b3ed7b4111"
                  "5a75047dcb.5548e845b6.76024a34.14d5b3.80c426.51db871c3a"
                  "825fa8acfc.5331.00caf8bbbd.107c4b.c291.126d1d010e"]]
    (doseq [s subjects]
      (is (= [nil false] (st/insert! t s 22)))
      (is (= [22 true] (st/lookup t s)) (str "should find " s)))))

(deftest match-tsep-second-then-partial-part-bug
  ;; Regression: stree.go:607. Exercises the partial-consumption dance
  ;; in match-parts.
  (let [t (st/make)]
    (doseq [s ["foo.xxxxx.foo1234.zz"
               "foo.yyy.foo123.zz"
               "foo.yyybar789.zz"
               "foo.yyy.foo12345.zz"
               "foo.yyy.foo12345.yy"
               "foo.yyy.foo123456789.zz"]]
      (st/insert! t s 22))
    (is (= 1 (count-match t "foo.*.foo123456789.*")))
    (is (= 0 (count-match t "foo.*.*.zzz.foo.>")))))

(deftest match-multiple-wildcard-basic
  (let [t (st/make)]
    (st/insert! t "A.B.C.D.0.G.H.I.0" 22)
    (st/insert! t "A.B.C.D.1.G.H.I.0" 22)
    (is (= 1 (count-match t "A.B.*.D.1.*.*.I.0")))))

(deftest match-invalid-wildcard
  ;; Wildcards at non-token positions are stored as literal bytes and
  ;; only match via `>` or other literal patterns.
  (let [t (st/make)]
    (st/insert! t "foo.123" 22)
    (st/insert! t "one.two.three.four.five" 22)
    (st/insert! t "'*.123" 22)
    (is (= 0 (count-match t "invalid.>")))
    (is (= 3 (count-match t ">")))
    (is (= 1 (count-match t "'*.*")))
    (is (= 0 (count-match t "'*.*.*'")))
    (doseq [filt ["`>`" "\">\"" "'>'" "'*.>'" "'*.>." "`invalid.>`" "'*.*'"]]
      (is (= 0 (count-match t filt)) (str "filter should not match: " filt)))))

(deftest random-track-entries
  ;; Reduced scale vs Go's 1000; plenty for regression signal.
  (let [t          (st/make)
        alphabet   "abcdef0123456789"
        rand-tok   (fn [] (apply str (repeatedly (+ 2 (rand-int 4))
                                                 #(rand-nth alphabet))))
        rand-subj  (fn [] (str/join "." (repeatedly (inc (rand-int 6)) rand-tok)))
        subjects   (atom #{})]
    (dotimes [_ 400]
      (let [s (rand-subj)]
        (when-not (@subjects s)
          (swap! subjects conj s)
          (st/insert! t s 22))))
    (is (= (count @subjects) (st/size t)))
    (doseq [s @subjects]
      (is (= [22 true] (st/lookup t s))))))

(deftest long-tokens-with-prefix-absorption
  ;; Regression: stree.go:688. Tokens longer than Go's internal 24-byte
  ;; prefix buffer. In Clojure we store arbitrarily-long strings so this
  ;; is mostly a smoke test for the absorption path post-delete.
  (let [t (st/make)]
    (st/insert! t "a1.aaaaaaaaaaaaaaaaaaaaaa0" 1)
    (st/insert! t "a2.0" 2)
    (st/insert! t "a1.aaaaaaaaaaaaaaaaaaaaaa1" 3)
    (st/insert! t "a2.1" 4)
    (st/delete! t "a2.0")
    (st/delete! t "a2.1")
    (is (= 2 (st/size t)))
    (is (= [1 true] (st/lookup t "a1.aaaaaaaaaaaaaaaaaaaaaa0")))
    (is (= [3 true] (st/lookup t "a1.aaaaaaaaaaaaaaaaaaaaaa1")))))

(deftest match-no-callback-dupe
  ;; Regression: stree.go:843. No subject should be passed to the cb
  ;; more than once per match call.
  (let [t (st/make)]
    (doseq [s ["foo.bar.A" "foo.bar.B" "foo.bar.C" "foo.bar.>"]]
      (st/insert! t s 1))
    (doseq [filt [">" "foo.>" "foo.bar.>"]]
      (let [seen (atom [])]
        (st/match t filt (fn [s _v] (swap! seen conj s)))
        (is (= (count @seen) (count (set @seen)))
            (str "dupes for filter: " filt))))))

(deftest insert-longer-leaf-suffix-with-trailing-nulls
  ;; Regression: stree.go:879. Trailing null bytes in subjects.
  (let [t     (st/make)
        subj  (apply str "foo.bar.baz_" (repeat 10 \u0000))
        subj2 (str subj (apply str (repeat 10 \u0000)))]
    (st/insert! t subj  1)
    (st/insert! t subj2 2)
    (is (= [1 true] (st/lookup t subj)))
    (is (= [2 true] (st/lookup t subj2)))))

(deftest insert-with-no-pivot-rejected
  ;; stree.go:905. Byte 127 (DEL) is a sentinel and must be rejected.
  (let [t    (st/make)
        subj (str "foo.bar.baz." \u007f)]
    (is (= [nil false] (st/insert! t subj 22)))
    (is (zero? (st/size t)))))

(deftest match-has-fwc-no-panic
  ;; stree.go:916. "." as a filter hits gen-parts edge cases.
  (let [t (st/make)]
    (st/insert! t "foo" 1)
    (is (= 0 (count-match t ".")))))

(deftest lazy-intersect-basic
  (let [t1 (st/make) t2 (st/make)]
    (doseq [[s v] [["foo.bar" 1]
                   ["foo.bar.baz.qux" 1]
                   ["bar" 1]
                   ["a.b.c" 1]
                   ["a.b.ee" 1]
                   ["bb.c.d" 1]]]
      (st/insert! t1 s v))
    (doseq [[s v] [["foo.bar" 1]
                   ["foo.bar.baz.qux" 1]
                   ["baz" 1]
                   ["a.b.d" 1]
                   ["a.b.e" 1]
                   ["b.c.d" 1]
                   ["foo.bar.baz.qux.alice" 1]
                   ["foo.bar.baz.qux.bob" 1]]]
      (st/insert! t2 s v))
    (let [seen (atom #{})]
      (st/lazy-intersect t1 t2 (fn [k _v1 _v2] (swap! seen conj k)))
      (is (= #{"foo.bar" "foo.bar.baz.qux"} @seen)))))

(deftest lazy-intersect-picks-smaller
  (let [large (st/make)
        small (st/make)]
    (dotimes [i 100] (st/insert! large (str "large." i) i))
    (st/insert! small "large.5"    500)
    (st/insert! small "large.10"   1000)
    (st/insert! small "large.50"   5000)
    (st/insert! small "small.only" 999)
    (let [hits (atom [])]
      (st/lazy-intersect large small
                         (fn [k v1 v2] (swap! hits conj [k v1 v2])))
      (is (= 3 (count @hits)))
      (is (= #{["large.5"  5  500]
               ["large.10" 10 1000]
               ["large.50" 50 5000]}
             (set @hits))))))

(deftest delete-short-subject-no-panic
  ;; stree.go:960. Subject shorter than a node's prefix should be a
  ;; well-behaved no-op.
  (let [t (st/make)]
    (st/insert! t "foo.bar.baz" 1)
    (st/insert! t "foo.bar.qux" 2)
    (is (= [nil false] (st/delete! t "foo.bar")))
    (is (= [1 true] (st/lookup t "foo.bar.baz")))
    (is (= [2 true] (st/lookup t "foo.bar.qux")))))

(deftest clear-resets-tree
  (let [t (st/make)]
    (is (zero? (st/size t)))
    (is (identical? t (st/clear! t)))
    (st/insert! t "foo.bar" 1)
    (st/insert! t "foo.baz" 2)
    (st/insert! t "bar.baz" 3)
    (is (= 3 (st/size t)))
    (st/clear! t)
    (is (zero? (st/size t)))
    (is (nil? (:root @t)))
    (is (= [nil false] (st/lookup t "foo.bar")))
    (is (= [nil false] (st/insert! t "new.entry" 42)))
    (is (= 1 (st/size t)))
    (is (= [42 true] (st/lookup t "new.entry")))))

(deftest find-edge-cases
  (let [t (st/make)]
    (st/insert! t "foo.bar.baz" 1)
    (st/insert! t "foo"         2)
    (is (= [nil false] (st/lookup t "")))))

(deftest insert-complex-edge-cases
  ;; stree.go:1719. Three-way same-pivot split (a, aa, aaa).
  (let [t (st/make)]
    (st/insert! t "a"   1)
    (st/insert! t "aa"  2)
    (st/insert! t "aaa" 3)
    (is (= 3 (st/size t)))
    (is (= [1 true] (st/lookup t "a")))
    (is (= [2 true] (st/lookup t "aa")))
    (is (= [3 true] (st/lookup t "aaa")))))

(deftest delete-edge-cases
  (let [t (st/make)]
    ;; delete on empty tree
    (is (= [nil false] (st/delete! t "foo")))
    (st/insert! t "foo" 1)
    ;; delete with empty subject
    (is (= [nil false] (st/delete! t "")))
    (is (= [1 true] (st/lookup t "foo")))
    ;; delete with subject shorter than prefix
    (let [t2 (st/make)]
      (st/insert! t2 "verylongprefix.suffix"  1)
      (st/insert! t2 "verylongprefix.suffix2" 2)
      (is (= [nil false] (st/delete! t2 "very"))))))

(deftest match-edge-cases
  (let [t (st/make)]
    (st/insert! t "foo.bar" 1)
    ;; nil callback
    (is (nil? (st/match t "foo.*" nil)))
    ;; empty filter
    (let [n (atom 0)]
      (st/match t "" (fn [_s _v] (swap! n inc)))
      (is (zero? @n)))))

(deftest iter-edge-cases
  (let [t (st/make)]
    (doseq [s ["a.b.c" "a.b.d" "a.c.d" "b.c.d"]]
      (st/insert! t s 1))
    (let [n (atom 0)]
      (st/iter-fast t (fn [_s _v] (swap! n inc) (< @n 2)))
      (is (= 2 @n)))))

(deftest match-complex-edge-cases
  (let [t (st/make)]
    (st/insert! t "foo.bar.baz" 1)
    (st/insert! t "foo.bar.qux" 2)
    (st/insert! t "foo.baz.bar" 3)
    (st/insert! t "bar.foo.baz" 4)
    (is (= 2 (count-match t "foo.bar.>")))))

(deftest iter-complex-tree
  (let [t (st/make)]
    (dotimes [i 20]
      (st/insert! t (str "level1.level2.level3.item" i) i))
    (let [n (atom 0)]
      (st/iter-ordered t (fn [_s _v] (swap! n inc) true))
      (is (= 20 @n)))))

(deftest iter-empty-tree-fires-zero-callbacks
  (let [t (st/make)
        n (atom 0)]
    (st/iter-ordered t (fn [_ _] (swap! n inc) true))
    (st/iter-fast    t (fn [_ _] (swap! n inc) true))
    (is (zero? @n))))
