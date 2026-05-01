(ns toolkit.llm.cli-test
  "Tests for the `claude -p` shell-out adapter. The process invocation
   is hidden behind a dynamic var (`*invoke-claude*`) so tests run
   without ever spawning a real subprocess. `call-json!` caches by
   default; the existing unit tests pass `:bypass? true` so the cache
   layer is inert and `*invoke-claude*` is invoked on every call. The
   dedicated cache-routing block at the bottom enables the cache
   against a per-test tmpdir to verify hit/miss/bypass semantics."
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [toolkit.llm.cache :as cache]
            [toolkit.llm.cli :as cli]))

(defn- wrap-success [m]
  {:exit 0
   :err  ""
   :out  (json/write-str
           {:type "result" :subtype "success" :is_error false
            :result "Done!"
            :structured_output m})})

(defn- wrap-failure []
  {:exit 0 :err ""
   :out  (json/write-str {:type "result" :is_error true
                          :result "Not logged in · Please run /login"})})

;; --- happy path -----------------------------------------------------------

(deftest call-json-happy-path
  (let [captured (atom nil)]
    (with-redefs [cli/*invoke-claude*
                  (fn [argv stdin]
                    (reset! captured {:argv argv :stdin stdin})
                    (wrap-success {:topic "rust" :intensity 7}))]
      (let [r (cli/call-json! {:system "be terse"
                               :user "classify this comment"
                               :schema {:type "object"
                                        :properties {:topic {:type "string"}
                                                     :intensity {:type "integer"}}
                                        :required [:topic :intensity]}
                               :model "claude-haiku-4-5" :bypass? true})]
        (testing "result is parsed JSON with kebab-case keys"
          (is (= "rust" (:topic r)))
          (is (= 7 (:intensity r))))
        (testing "user message goes via stdin"
          (is (= "classify this comment" (:stdin @captured))))
        (testing "argv carries model + format + schema + system"
          (let [argv (:argv @captured)]
            (is (= "claude" (first argv)))
            (is (some #{"-p"} argv))
            (is (= "claude-haiku-4-5" (nth argv (inc (.indexOf argv "--model")))))
            (is (= "json" (nth argv (inc (.indexOf argv "--output-format")))))
            (is (some #{"--json-schema"} argv))
            (is (= "be terse" (nth argv (inc (.indexOf argv "--append-system-prompt")))))))))))

(deftest underscore-keys-converted-to-dashes
  (with-redefs [cli/*invoke-claude*
                (fn [_ _] (wrap-success {:is_mind_change true :confidence 0.9}))]
    (let [r (cli/call-json! {:system "" :user ""
                             :schema {:type "object"}
                             :model "claude-haiku-4-5" :bypass? true})]
      (is (= true (:is-mind-change r)))
      (is (= 0.9  (:confidence r))))))

(deftest keys-snake-leaves-underscores-alone
  (with-redefs [cli/*invoke-claude*
                (fn [_ _] (wrap-success {:drift_score 8 :drift_target "tangent"}))]
    (let [r (cli/call-json! {:system "" :user ""
                             :schema {:type "object"}
                             :model "claude-haiku-4-5"
                             :keys :snake})]
      (is (= 8 (:drift_score r)))
      (is (= "tangent" (:drift_target r))))))

(deftest result-can-be-prefixed-with-prose
  (testing "fallback path — no structured_output, but JSON in result prose"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str
                            {:type "result" :is_error false
                             :result (str "Sure, here you go:\n"
                                          (json/write-str {:answer 42})
                                          "\nLet me know if you need more.")})})]
      (let [r (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5" :bypass? true})]
        (is (= 42 (:answer r)))))))

(deftest structured-output-wins-over-result-prose
  (testing "with both fields set, structured_output is preferred"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str
                            {:type "result" :is_error false
                             :result "Done!"
                             :structured_output {:answer 7}})})]
      (let [r (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5" :bypass? true})]
        (is (= 7 (:answer r)))))))

;; --- error paths ----------------------------------------------------------

(deftest call-json-not-logged-in-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _] (wrap-failure))]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5" :bypass? true})))))

(deftest call-json-non-zero-exit-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _]
                                      {:exit 1 :err "boom" :out ""})]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5" :bypass? true})))))

(deftest call-json-malformed-result-returns-nil
  (testing "wrapper parses but inner result isn't valid JSON"
    (with-redefs [cli/*invoke-claude*
                  (fn [_ _]
                    {:exit 0 :err ""
                     :out (json/write-str {:type "result" :is_error false
                                           :result "not json at all"})})]
      (is (nil? (cli/call-json! {:system "" :user ""
                                 :schema {:type "object"}
                                 :model "claude-haiku-4-5" :bypass? true}))))))

(deftest call-json-malformed-wrapper-returns-nil
  (with-redefs [cli/*invoke-claude* (fn [_ _]
                                      {:exit 0 :err ""
                                       :out "totally garbage"})]
    (is (nil? (cli/call-json! {:system "" :user ""
                               :schema {:type "object"}
                               :model "claude-haiku-4-5" :bypass? true})))))

;; --- defaults -------------------------------------------------------------

(deftest model-defaults-to-haiku
  (let [captured (atom nil)]
    (with-redefs [cli/*invoke-claude*
                  (fn [argv _]
                    (reset! captured argv)
                    (wrap-success {}))]
      (cli/call-json! {:system "" :user ""
                       :schema {:type "object"}
                       :bypass? true})
      (is (= "claude-haiku-4-5"
             (nth @captured (inc (.indexOf @captured "--model"))))))))

;; --- cache-routing semantics ---------------------------------------------
;;
;; These tests redirect `cache/default-store` at a per-test tmpdir
;; LMDB env. They verify that identical opts hit cache, that argument
;; changes bust it, and that `:bypass? true` skips the cache entirely.

(defn- tmp-cache []
  (let [d (io/file (System/getProperty "java.io.tmpdir")
                   (str "cli-cache-test-" (System/nanoTime)))]
    (.mkdirs d)
    {:dir d
     :store (cache/open (.getPath d) {:map-size (* 16 1024 1024)})}))

(defn- cleanup-cache [{:keys [dir store]}]
  (cache/close store)
  (doseq [^java.io.File f (reverse (file-seq dir))] (.delete f)))

(defn- with-temp-cache [f]
  (let [tc (tmp-cache)]
    (with-redefs [cache/default-store (constantly (:store tc))]
      (try (f)
           (finally (cleanup-cache tc))))))

(defn- counting-claude
  "An *invoke-claude* fixture that increments `calls` and returns
   `(produce)` each time."
  [calls produce]
  (fn [_argv _stdin]
    (swap! calls inc)
    (produce)))

(deftest cache-hits-on-identical-opts
  (with-temp-cache
    (fn []
      (let [calls (atom 0)]
        (with-redefs [cli/*invoke-claude*
                      (counting-claude calls
                                       #(wrap-success {:topic "rust"
                                                       :intensity 7}))]
          (let [opts {:system "be terse" :user "classify"
                      :schema {:type "object"}
                      :model "claude-haiku-4-5"
                      :cache-tag "test"}
                r1 (cli/call-json! opts)
                r2 (cli/call-json! opts)]
            (is (= {:topic "rust" :intensity 7} r1))
            (is (= r1 r2))
            (is (= 1 @calls)
                "second call should be served from cache, not invoke claude")))))))

(deftest cache-busts-on-input-change
  (with-temp-cache
    (fn []
      (let [calls (atom 0)
            base  {:system "sys" :user "u" :schema {:type "object"}
                   :model "claude-haiku-4-5" :cache-tag "test"}]
        (with-redefs [cli/*invoke-claude*
                      (counting-claude calls
                                       #(wrap-success {:answer "x"}))]
          (cli/call-json! base)
          (cli/call-json! (assoc base :user "different"))
          (cli/call-json! (assoc base :system "different"))
          (cli/call-json! (assoc base :model "claude-sonnet-4-6"))
          (cli/call-json! (assoc base :keys :snake))
          (cli/call-json! (assoc base :cache-tag "other"))
          (is (= 6 @calls)
              "every distinct opts shape should produce a cache miss"))))))

(deftest bypass-skips-cache-for-one-call
  (with-temp-cache
    (fn []
      (let [calls (atom 0)
            opts  {:system "" :user "" :schema {:type "object"}
                   :model "claude-haiku-4-5" :cache-tag "test"}]
        (with-redefs [cli/*invoke-claude*
                      (counting-claude calls #(wrap-success {:n @calls}))]
          (cli/call-json! opts)
          (cli/call-json! opts)
          (is (= 1 @calls) "second identical call hits cache")
          (cli/call-json! (assoc opts :bypass? true))
          (is (= 2 @calls) ":bypass? true bypasses the cache"))))))

(deftest bypass-does-not-write-to-cache
  (with-temp-cache
    (fn []
      (let [calls (atom 0)
            opts  {:system "" :user "" :schema {:type "object"}
                   :model "claude-haiku-4-5" :cache-tag "test"}]
        (with-redefs [cli/*invoke-claude*
                      (counting-claude calls #(wrap-success {:n @calls}))]
          (cli/call-json! (assoc opts :bypass? true))
          (cli/call-json! opts)
          (is (= 2 @calls)
              ":bypass? true skips writing too — the next default call still misses"))))))

(deftest cache-skips-nil-results
  (with-temp-cache
    (fn []
      (let [calls (atom 0)
            opts  {:system "" :user "" :schema {:type "object"}
                   :model "claude-haiku-4-5" :cache-tag "test"}]
        (with-redefs [cli/*invoke-claude* (counting-claude calls
                                                           #(wrap-failure))]
          (is (nil? (cli/call-json! opts)))
          (is (nil? (cli/call-json! opts)))
          (is (= 2 @calls)
              "nil results from transient failures are not cached"))))))

(deftest cache-roundtrips-snake-keys
  (testing "regression: :keys :snake is part of the cache key, so a snake call doesn't reuse a kebab entry"
    (with-temp-cache
      (fn []
        (let [calls (atom 0)
              opts  {:system "" :user "" :schema {:type "object"}
                     :model "claude-haiku-4-5" :cache-tag "test"}]
          (with-redefs [cli/*invoke-claude*
                        (counting-claude calls
                                         #(wrap-success {:edge_type "agree"}))]
            (let [k (cli/call-json! (assoc opts :keys :kebab))
                  s (cli/call-json! (assoc opts :keys :snake))]
              (is (= "agree" (:edge-type k)))
              (is (= "agree" (:edge_type s)))
              (is (= 2 @calls)
                  ":kebab and :snake keys hash to different cache entries"))))))))
