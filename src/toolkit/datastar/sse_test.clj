(ns toolkit.datastar.sse-test
  (:require [clojure.test :refer [deftest is]]
            [toolkit.datastar.sse :as sse]))

(deftest parse-accept-encoding-basics
  (is (= ["br"]                     (sse/parse-accept-encoding "br")))
  (is (= ["br" "gzip" "identity"]   (sse/parse-accept-encoding "br,gzip,identity")))
  (is (= ["br" "gzip"]              (sse/parse-accept-encoding "br;q=1.0,gzip;q=0.5"))
      "strips the quality parameter")
  (is (= ["br" "gzip"]              (sse/parse-accept-encoding " br , gzip "))
      "trims whitespace"))

(deftest preferred-content-encoding-picks-first-server-pref-the-client-accepts
  (is (= "br"
         (sse/preferred-content-encoding "br,gzip" ["br" "gzip" "identity"])))
  (is (= "br"
         (sse/preferred-content-encoding "gzip,br" ["br" "gzip" "identity"]))
      "server ranking wins over client ordering")
  (is (= "gzip"
         (sse/preferred-content-encoding "gzip" ["br" "gzip" "identity"]))
      "skips unavailable server preferences")
  (is (= "identity"
         (sse/preferred-content-encoding "identity" ["br" "gzip" "identity"]))))

(deftest preferred-content-encoding-falls-back-to-identity
  (is (= "identity"
         (sse/preferred-content-encoding "deflate" ["br" "gzip" "identity"]))
      "no overlap → identity"))

(deftest preferred-content-encoding-handles-nil-accept-header
  (is (= "identity"
         (sse/preferred-content-encoding nil ["br" "gzip" "identity"]))))
