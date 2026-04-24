(ns toolkit.os-guard
  "Refuse to run on macOS.

  The app expects a Linux container: lmdbjava, brotli4j, and parquet/hadoop
  each pull in native libraries and JVM --add-opens flags, and we don't want
  a JVM accidentally started on the macOS host to exercise the wrong
  native-lib variant or pick up stray state. Required from every first-party
  entry point (`demo.server`, `user`, `toolkit.bench`, `toolkit.test-runner`)
  so the check runs before any component starts, no matter which alias
  launched the JVM.

  To simulate a macOS run from inside the Linux container, override the JVM
  property so `os.name` looks like a Mac:

    clojure -J-Dos.name=Mac -M:run
    clojure -J-Dos.name=Mac -M:dev
    clojure -J-Dos.name=Mac -M:bench
    clojure -J-Dos.name=Mac -X:test

  Each should print the refusal and exit 1 before any app code runs.")

(when (.startsWith ^String (System/getProperty "os.name") "Mac")
  (binding [*out* *err*]
    (println "Refusing to run on macOS — this program expects a Linux container."))
  (System/exit 1))
