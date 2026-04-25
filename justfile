dev:
    @# rebel provides its own readline, while clj wraps clojure with rlwrap.
    clojure -M:dev:rebel

run:
    clj -M:run

prep:
    clj -X:deps prep

lint path="src":
    clojure -M:kondo --lint {{path}}

test:
    clj -X:test

test-costly:
    clj -X:test-costly
