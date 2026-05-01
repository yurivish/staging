(ns hn-title-editorialize.core-test
  "Unit + end-to-end tests for hn-title-editorialize. Live HTTP is
   stubbed via with-redefs."
  (:require [clojure.test :refer [deftest is testing]]
            [hn-title-editorialize.core :as core]
            [toolkit.datapotamus.flow :as flow]))

;; --- HTML extraction --------------------------------------------------------

(deftest extract-source-title-prefers-og
  (testing "og:title wins over twitter:title and <title>"
    (let [html (str "<html><head>"
                    "<title>Plain Title</title>"
                    "<meta property=\"og:title\" content=\"OG Title\">"
                    "<meta name=\"twitter:title\" content=\"Twitter Title\">"
                    "</head></html>")]
      (is (= "OG Title" (core/extract-source-title html)))))
  (testing "twitter:title wins over <title> when og missing"
    (let [html (str "<title>Plain</title>"
                    "<meta name=\"twitter:title\" content=\"Twitter\">")]
      (is (= "Twitter" (core/extract-source-title html)))))
  (testing "<title> as last resort"
    (is (= "Just Title" (core/extract-source-title "<title>Just Title</title>"))))
  (testing "nil for empty html"
    (is (nil? (core/extract-source-title nil)))
    (is (nil? (core/extract-source-title ""))))
  (testing "decodes basic entities"
    (is (= "A & B"  (core/extract-source-title "<title>A &amp; B</title>")))
    (is (= "\"hi\"" (core/extract-source-title "<title>&quot;hi&quot;</title>")))
    (is (= "—"      (core/extract-source-title "<title>&#8212;</title>"))))
  (testing "case-insensitive tag matching"
    (is (= "X" (core/extract-source-title "<TITLE>X</TITLE>")))))

(deftest strip-source-boilerplate-removes-known-suffixes
  (is (= "Some Article" (core/strip-source-boilerplate "Some Article - Wikipedia")))
  (is (= "An Op-Ed"     (core/strip-source-boilerplate "An Op-Ed | The New York Times")))
  (is (= "Untouched"    (core/strip-source-boilerplate "Untouched"))))

;; --- Metrics ----------------------------------------------------------------

(deftest metrics-identical
  (let [m (core/metrics "Hello World" "Hello World")]
    (is (true?  (:exact-match m)))
    (is (true?  (:ci-match m)))
    (is (= 1.0  (:token-jaccard m)))
    (is (zero? (:normalized-edit-distance m)))
    (is (= 1.0  (:lcs-token-fraction m)))
    (is (= 1.0  (:length-ratio m)))
    (is (= :identical (:divergence-flag m)))))

(deftest metrics-ci-only
  (let [m (core/metrics "Hello WORLD" "hello world")]
    (is (false? (:exact-match m)))
    (is (true?  (:ci-match m)))
    (is (= :near-identical (:divergence-flag m)))))

(deftest metrics-clipped
  (testing "HN title is much shorter; tokens largely overlap"
    (let [hn  "Some Important Article About Stuff"
          src "Some Important Article About Stuff: A Very Long Source Title With Many More Words"
          m   (core/metrics hn src)]
      (is (false? (:exact-match m)))
      (is (< (:length-ratio m) 0.7))
      (is (> (:lcs-token-fraction m) 0.6))
      (is (= :clipped (:divergence-flag m))))))

(deftest metrics-expanded
  (testing "HN title is meaningfully longer than source"
    (let [src "Short Title"
          hn  "Short Title But With Lots Of Editorial Context Added Here"
          m   (core/metrics hn src)]
      (is (> (:length-ratio m) 1.3))
      (is (= :expanded (:divergence-flag m))))))

(deftest metrics-divergent
  (testing "tokens barely overlap → divergent"
    (let [m (core/metrics "Cats are wonderful" "Industrial robotics in 2024")]
      (is (< (:token-jaccard m) 0.2))
      (is (= :divergent (:divergence-flag m))))))

(deftest metrics-source-title-missing
  (let [m (core/metrics "Anything" nil)]
    (is (true? (:source-title-missing? m)))
    (is (= :source-title-missing (:divergence-flag m)))))

(deftest bracket-tags-detected
  (let [m (core/metrics "[Show HN] My new tool" "My new tool")]
    (is (some #{"Show HN"} (:bracket-tags m))))
  (let [m (core/metrics "An Article [2019]" "An Article")]
    (is (some #{"year"} (:bracket-tags m))))
  (let [m (core/metrics "An Article [paywall]" "An Article")]
    (is (some #{"paywall"} (:bracket-tags m)))))

(deftest paren-asides-flag-additions
  (let [m (core/metrics "An Article (announcement)" "An Article")]
    (is (some #{"announcement"} (:paren-asides m))))
  (testing "if the parenthetical also appears in source, it's not flagged"
    (let [m (core/metrics "Article (2019 update)" "Article (2019 update) on something")]
      (is (empty? (:paren-asides m))))))

(deftest q-and-excl-deltas
  (let [m (core/metrics "Why are tabs better?" "Tabs are better")]
    (is (= 1 (:q-mark-delta m))))
  (let [m (core/metrics "Tabs!!" "Tabs.")]
    (is (= 2 (:excl-mark-delta m)))))

;; --- E2E with stubbed HTTP --------------------------------------------------

(defn- stub-get-json [m]
  (fn [url]
    (or (get m url)
        (throw (ex-info (str "missing fixture json url: " url) {:url url})))))

(defn- stub-http-get-text [m]
  (fn [url] (get m url)))

(deftest end-to-end-with-stubs
  (let [hn-data
        {"https://hacker-news.firebaseio.com/v0/topstories.json" [1 2 3]
         "https://hacker-news.firebaseio.com/v0/item/1.json"
         {:id 1 :type "story" :title "Hello World"
          :url "https://example.com/a"}
         "https://hacker-news.firebaseio.com/v0/item/2.json"
         {:id 2 :type "story" :title "[Show HN] My Thing"
          :url "https://example.com/b"}
         "https://hacker-news.firebaseio.com/v0/item/3.json"
         ;; URL-less story should be filtered out.
         {:id 3 :type "story" :title "Ask HN: How do you?"}}
        page-data
        {"https://example.com/a" "<title>Hello World</title>"
         "https://example.com/b" (str "<meta property=\"og:title\" "
                                      "content=\"My Thing - The Project\">"
                                      "<title>My Thing</title>")}]
    (with-redefs [core/get-json        (stub-get-json hn-data)
                  core/http-get-text   (stub-http-get-text page-data)]
      (let [res (flow/run-seq (core/build-flow {:n-stories 3 :fetch-workers 2})
                              [:tick])
            summary (last (first (:outputs res)))]
        (is (= :completed (:state res)))
        (is (= 2 (:n-stories summary))
            "URL-less story was filtered out before metrics")
        (is (= 2 (:n-with-source-title summary)))
        (testing "the identical pair lands in :identical"
          (is (pos? (get-in summary [:flag-counts :identical]))))
        (testing "the second pair has og:title differing → not identical"
          (let [row (some #(when (= 2 (:story-id %)) %) (:rows summary))]
            (is (= "[Show HN] My Thing" (:hn-title row)))
            (is (= "My Thing - The Project" (:source-title row)))
            (is (some #{"Show HN"} (-> row :metrics :bracket-tags)))))))))

(deftest end-to-end-handles-no-source-title
  (let [hn-data
        {"https://hacker-news.firebaseio.com/v0/topstories.json" [1]
         "https://hacker-news.firebaseio.com/v0/item/1.json"
         {:id 1 :type "story" :title "Some Article"
          :url "https://example.com/x"}}
        ;; page returned no html (e.g., timeout) → http-get-text returns nil
        page-data {}]
    (with-redefs [core/get-json      (stub-get-json hn-data)
                  core/http-get-text (stub-http-get-text page-data)]
      (let [res (flow/run-seq (core/build-flow {:n-stories 1 :fetch-workers 2})
                              [:tick])
            summary (last (first (:outputs res)))]
        (is (= :completed (:state res)))
        (is (= 1 (:n-stories summary)))
        (is (= 0 (:n-with-source-title summary)))
        (is (= 1 (get-in summary [:flag-counts :source-title-missing])))))))

(deftest end-to-end-flag-rates-sum-to-one
  (let [hn-data
        {"https://hacker-news.firebaseio.com/v0/topstories.json" [1 2 3]
         "https://hacker-news.firebaseio.com/v0/item/1.json"
         {:id 1 :type "story" :title "A B C" :url "https://example.com/1"}
         "https://hacker-news.firebaseio.com/v0/item/2.json"
         {:id 2 :type "story" :title "A B C" :url "https://example.com/2"}
         "https://hacker-news.firebaseio.com/v0/item/3.json"
         {:id 3 :type "story" :title "X Y Z" :url "https://example.com/3"}}
        page-data
        {"https://example.com/1" "<title>A B C</title>"
         "https://example.com/2" "<title>A B C and More Words Added Here Too</title>"
         "https://example.com/3" "<title>Q R S</title>"}]
    (with-redefs [core/get-json      (stub-get-json hn-data)
                  core/http-get-text (stub-http-get-text page-data)]
      (let [res     (flow/run-seq (core/build-flow {:n-stories 3 :fetch-workers 2})
                                  [:tick])
            summary (last (first (:outputs res)))
            rates   (vals (:flag-rates summary))
            total   (reduce + 0.0 rates)]
        (is (= :completed (:state res)))
        (is (< (Math/abs (- 1.0 total)) 1e-9)
            (str "rates should sum to 1: " (:flag-rates summary)))))))
