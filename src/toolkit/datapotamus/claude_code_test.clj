(ns toolkit.datapotamus.claude-code-test
  "Sanity checks for the Claude Code step. Real-binary calls; cost-minimized
   with `:bare? true` (skip project-context loading) and `:model \"haiku\"`.
   Auth comes from `claude.key` in the JVM cwd, passed to the subprocess
   via `:env` so we don't pollute the global environment.

   Tests skip gracefully when `claude.key` is missing — keeps CI green
   without auth setup."
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is]]
            [toolkit.datapotamus.claude-code :as cc]))

(defn- claude-key []
  (let [f (java.io.File. "claude.key")]
    (when (.exists f) (str/trim (slurp f)))))

(defn- base-opts [k]
  {:model     "haiku"
   :max-turns 1
   :bare?     true
   :env       {"ANTHROPIC_API_KEY" k}})

#_ ; disabled: redundant with run!-json-schema, which exercises the same path.
(deftest run!-roundtrip
  (if-let [k (claude-key)]
    (let [r (cc/run! (assoc (base-opts k)
                            :prompt "Reply with exactly the word: hi"))]
      (is (= "hi" (-> r :result str/trim str/lower-case)))
      (is (string? (:session-id r)))
      (is (false? (:is-error r)))
      (is (number? (:total-cost-usd r))))
    (println "skipping run!-roundtrip: claude.key not present")))

(deftest ^:costly run!-json-schema
  (cond
    (not (System/getProperty "test.costly"))
    (println "skipping run!-json-schema: requires -Dtest.costly (see :test-costly alias in deps.edn)")

    (not (claude-key))
    (println "skipping run!-json-schema: claude.key not present")

    :else
    (let [r (cc/run! (assoc (base-opts (claude-key))
                            :max-turns 3   ; schema-conforming output needs ≥2 turns
                            :prompt "Set greeting to \"hi\" and answer to 42."
                            :json-schema {:type       "object"
                                          :properties {:greeting {:type "string"}
                                                       :answer   {:type "integer"}}
                                          :required   ["greeting" "answer"]}))
          out (:structured-output r)]
      (is (false? (:is-error r)))
      (is (map? out))
      (is (= "hi" (str/lower-case (str (:greeting out)))))
      (is (= 42 (:answer out))))))
