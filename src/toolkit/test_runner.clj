(ns toolkit.test-runner
  "Wraps cognitect.test-runner so the OS guard loads before test discovery —
  otherwise tests that don't transitively require app code (e.g. toolkit.frng-test)
  would run on macOS."
  (:refer-clojure :exclude [test])
  (:require [toolkit.os-guard]
            [cognitect.test-runner.api :as tr]))

(defn test [opts] (tr/test opts))
