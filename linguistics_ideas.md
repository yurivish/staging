# Linguistics ideas

## The seed

What pulled me in: statistically improbable phrases / collocations.
Specific papers I want to absorb — Dunning's "Accurate Methods for the
Statistics of Surprise and Coincidence" and Quasthoff & Wolff's Poisson
collocation measure. Adjacent: I want interpolated Kneser-Ney smoothing
in the toolkit too. Standard primitives (n-grams, stemming / word
canonicalization) sit underneath.

Setting: Clojure, small corpora — thousands of podcast transcripts and
~a month of Hacker News comments, not more. Costs will be dominated by
LLM processing, so the classical pieces don't have to be fast.

The angle I want to explore: LLMs as a *pre-normalization* layer
(entity canonicalization, coreference, structure extraction) on top of
which the classical algorithms operate — interpretable statistics over
LLM-cleaned input.

## Survey

**Collocation / phrase detection** — Dunning log-likelihood ratio
(G², 1993; same 2×2 contingency-table machinery serves keyness too);
Quasthoff & Wolff Poisson collocation measure (2006; ≈ LLR in practice);
iterative phrase merging (word2vec-style PMI discount, cheap and
effective on bigrams → trigrams).

**Keyness vs. reference corpus** — Weighted log-odds with Dirichlet
prior (Monroe, Colaresi & Quinn, "Fightin' Words" 2008; robust on small
corpora).

**N-gram language-model smoothing** — Interpolated / Modified Kneser-Ney
(Chen & Goodman 1998); Jelinek-Mercer linear interpolation as a simpler
baseline.

**LLM pre-normalization** — entity canonicalization ("GPT-4" / "GPT4" /
"gpt-4" → one ID), coreference resolution, structure extraction (claims,
quotes, code blocks, URLs). Classical statistics then run on the
canonicalized stream.
