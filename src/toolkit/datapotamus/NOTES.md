What landed beyond the plan:

- Bumped core.async to 1.10.870-alpha2 (flow namespace isn't in 1.9.x).
- Anchored .gitignore so /datapotamus matches only the top-level Go project, not src/toolkit/datapotamus/.
- Runner calls flow/resume after flow/start (flows start paused — undocumented).
- Runner dropped the in-flight counter; completion is pure idle-timeout.
- Sink emits :recv + :sink-wrote + :success for trace symmetry with step-proc.
- Daemon walks dirs on {:dir? true} events (new-file drops only fire that, not :dir? false).

Backlog the final reviewer flagged (all non-blocking):

1. proc/derive shadows clojure.core/derive. Rename to child-msg or derive-msg to kill the load-time warning.
2. XOR-token completion is now required before adding fan-out beyond what the demo's repeat3 does.
3. sqlite-sink rowid extraction has 4 fallbacks; a {:builder-fn …as-unqualified-lower-maps} option would collapse to one.
4. The daemon's directory-walk branch isn't test-covered.
5. TraceCollector can't be restarted after stop (fine today; note it).
