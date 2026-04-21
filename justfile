dev:
    @# rebel provides its own readline, while clj wraps clojure with rlwrap.
    clojure -M:dev:rebel

run:
    clj -M:run

prep:
    clj -X:deps prep
