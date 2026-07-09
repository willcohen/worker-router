;; Copyright 2026 Will Cohen
;; SPDX-License-Identifier: Apache-2.0
;;
;; The single functional test suite for the pool. The library is authored
;; in squint (pool.cljc -> dist/index.mjs), so its tests are authored in
;; squint too. Runs on node via squint's cljs.test adapter
;; (node_modules/squint-cljs/src/squint/test.js): deftest registers via
;; test_registry; run-tests awaits any Promise the body returns (test_var
;; checks async_QMARK_ on the result). Assertion macro `is` covers
;; value-equality (via squint's = → dequal which is deep on JS
;; arrays/objects), thrown?, and thrown-with-msg?.
;;
;; The suite imports the pool directly from the squint-compiled build at
;; ../../dist/index.mjs. The browser-eval case additionally drives headless
;; chromium via Playwright to assert the bundle evaluates in a browser.

(ns pool-test
  (:require [cljs.test :as t :refer [deftest is]]
            ["node:events" :refer [once]]
            ["node:worker_threads" :refer [Worker]]
            ["node:fs" :refer [readFileSync rmSync]]
            ["node:fs/promises" :refer [readFile]]
            ["node:os" :refer [tmpdir]]
            ["node:path" :refer [join extname]]
            ["node:http" :refer [createServer]]
            ["node:url" :refer [fileURLToPath]]
            ["comlink" :as Comlink]
            ["comlink/dist/esm/node-adapter.mjs" :as node-adapter-mod]
            ["playwright" :refer [chromium]]
            ["../../dist/index.mjs" :refer [WorkerPool detectRuntime spawn]]))

(def nodeEndpoint (.-default node-adapter-mod))

(def bootstrap-url
  (.-href (new js/URL "../../dist/worker-bootstrap.mjs" js/import.meta.url)))
(def bootstrap-url-obj
  (new js/URL "../../dist/worker-bootstrap.mjs" js/import.meta.url))
(def echo-url
  (.-href (new js/URL "../fixtures/echo-handler.mjs" js/import.meta.url)))
(def counter-url
  (.-href (new js/URL "../fixtures/counter-handler.mjs" js/import.meta.url)))
(def factory-url
  (.-href (new js/URL "../fixtures/factory-handler.mjs" js/import.meta.url)))
(def init-tracker-url
  (.-href (new js/URL "../fixtures/init-tracker-handler.mjs" js/import.meta.url)))
(def destroy-tracker-url
  (.-href (new js/URL "../fixtures/destroy-tracker-handler.mjs" js/import.meta.url)))
(def exit-url
  (.-href (new js/URL "../fixtures/exit-handler.mjs" js/import.meta.url)))
(def noop-bootstrap-url
  (.-href (new js/URL "../fixtures/noop-bootstrap.mjs" js/import.meta.url)))
(def silent-bootstrap-url
  (.-href (new js/URL "../fixtures/silent-bootstrap.mjs" js/import.meta.url)))
(def hang-destroy-url
  (.-href (new js/URL "../fixtures/hang-destroy-handler.mjs" js/import.meta.url)))
(def slow-url
  (.-href (new js/URL "../fixtures/slow-handler.mjs" js/import.meta.url)))
(def crash-inflight-url
  (.-href (new js/URL "../fixtures/crash-inflight-handler.mjs" js/import.meta.url)))
(def window-crash-url
  (.-href (new js/URL "../fixtures/window-crash-handler.mjs" js/import.meta.url)))

;; Records any uncaughtException that reaches the host process. A worker that
;; dies in the bootstrap window used to surface its 'error' event here (an
;; unhandled 'error' on a node Worker is a host crash); the containment test
;; asserts nothing leaks through.
(def host-uncaught (js/Array.))
(.on js/process "uncaughtException"
     (fn [e] (.push host-uncaught (or (some-> e .-message) (str e)))))

(defn make-pool
  ([handlers] (make-pool handlers 2))
  ([handlers size]
   (.create WorkerPool
            #js {:size size :bootstrap bootstrap-url :handlers handlers})))

(deftest ^:async create-resolves-only-after-every-worker-is-ready
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 3))]
    (try
      (is (= 3 (.-size pool)))
      (is (= "node" (.-runtime pool)))
      (is (= #js ["echo"]
             (.sort (js/Array.from (.-registeredModules pool)))))
      (loop [i 0]
        (when (< i 3)
          (is (= "hi" (await (.echo (.-echo (.worker pool i)) "hi"))))
          (recur (inc i))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async exposed-modules-match-registered
  (let [pool (await
              (make-pool
               #js {:echo #js {:module echo-url}
                    :f    #js {:module factory-url :init #js {:prefix "p"}}}
               2))]
    (try
      (loop [i 0]
        (when (< i 2)
          (is (= i (await (.echo (.-echo (.worker pool i)) i))))
          (is (= "p:x" (await (.greet (.-f (.worker pool i)) "x"))))
          (recur (inc i))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async affinity-is-stable
  (let [pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 10}}}
               2))]
    (try
      (is (= 11 (await (.inc (.-counter (.worker pool 0))))))
      (is (= 12 (await (.inc (.-counter (.worker pool 0))))))
      (is (= 13 (await (.inc (.-counter (.worker pool 0))))))
      (is (= 10 (await (.get (.-counter (.worker pool 1))))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async any-picks-least-loaded-concurrent
  (let [size 4
        pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               size))]
    (try
      (let [promises (js/Array.from
                      #js {:length size}
                      (fn [_ _i] (.inc (.-counter (.any pool)))))
            results  (await (js/Promise.all promises))]
        (is (= #js [1 1 1 1] (.sort results))))
      (let [per-worker (js/Array.)]
        (loop [i 0]
          (when (< i size)
            (.push per-worker
                   (await (.get (.-counter (.worker pool i)))))
            (recur (inc i))))
        (is (= #js [1 1 1 1] (.sort per-worker))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async any-picks-least-loaded-sequential
  (let [size 4
        pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               size))]
    (try
      (let [results (js/Array.)]
        (loop [i 0]
          (when (< i size)
            (.push results (await (.inc (.-counter (.any pool)))))
            (recur (inc i))))
        (is (= #js [1 2 3 4] results)))
      (let [w0     (await (.get (.-counter (.worker pool 0))))
            others (await
                    (js/Promise.all
                     (.map #js [1 2 3]
                           (fn [i] (.get (.-counter (.worker pool i)))))))]
        (is (= 4 w0))
        (is (= #js [0 0 0] others)))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async any-routes-around-affinity
  (let [size 2
        pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               size))]
    (try
      (let [pinned (.inc (.-counter (.worker pool 0)))
            anyp   (.inc (.-counter (.any pool)))]
        (await (js/Promise.all #js [pinned anyp])))
      (is (= 1 (await (.get (.-counter (.worker pool 0)))))
          "worker 0 should only have received the pinned call")
      (is (= 1 (await (.get (.-counter (.worker pool 1)))))
          "worker 1 should have received the any() call")
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async in-flight-count-is-non-negative
  (let [size 3
        pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               size))]
    (try
      (let [calls (js/Array.)]
        (loop [i 0]
          (when (< i 12)
            (.push calls (.inc (.-counter (.any pool))))
            (recur (inc i))))
        (await (js/Promise.all calls)))
      (await (.inc (.-counter (.any pool))))
      (let [w0 (await (.get (.-counter (.worker pool 0))))]
        (is (>= w0 1) "worker 0 received at least the post-quiesce any() call"))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async nested-call-counting
  (let [size 2
        pool (await
              (.create WorkerPool
                       #js {:bootstrap bootstrap-url
                            :size size
                            :handlers
                            #js {:echo #js {:module echo-url}
                                 :counter #js {:module counter-url
                                               :init #js {:start 0}}}}))]
    (try
      (let [results (await
                     (js/Promise.all
                      #js [(.double (.-nested (.-echo (.any pool))) 1)
                           (.double (.-nested (.-echo (.any pool))) 2)
                           (.double (.-nested (.-echo (.any pool))) 3)
                           (.double (.-nested (.-echo (.any pool))) 4)]))]
        (is (= #js [2 4 6 8] results)))
      (await (.inc (.-counter (.any pool))))
      (is (= 1 (await (.get (.-counter (.worker pool 0))))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async claim-sequential-fanout
  (let [size 4
        pool (await (make-pool #js {:echo #js {:module echo-url}} size))]
    (try
      (let [claims (js/Array.from
                    #js {:length size}
                    (fn [_ _i] (.claim pool)))]
        (is (= #js [0 1 2 3] (.map claims (fn [c] (.-index c)))))
        (let [fifth (.claim pool)]
          (is (= 0 (.-index fifth)))
          ((.-release fifth))
          (let [sixth (.claim pool)]
            (is (= 0 (.-index sixth)))
            (.forEach claims (fn [c] ((.-release c))))
            ((.-release sixth)))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async claim-release-idempotent
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))]
    (try
      (let [c0 (.claim pool)
            c1 (.claim pool)]
        (is (= #js [0 1] #js [(.-index c0) (.-index c1)]))
        ((.-release c0))
        ((.-release c0)) ;; idempotent
        (let [c2 (.claim pool)]
          (is (= 0 (.-index c2)))
          ((.-release c1))
          ((.-release c2))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async claim-pending-share
  (let [size 4
        pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               size))]
    (try
      (let [c0 (.claim pool)
            c1 (.claim pool)]
        (is (= #js [0 1] #js [(.-index c0) (.-index c1)]))
        (await (.inc (.-counter (.any pool))))
        (is (= 1 (await (.get (.-counter (.worker pool 2))))))
        (is (= 0 (await (.get (.-counter (.worker pool 0))))))
        (is (= 0 (await (.get (.-counter (.worker pool 1))))))
        ((.-release c0))
        ((.-release c1)))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async claim-throws-after-terminate
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))]
    (await ((.-terminate pool)))
    (is (thrown-with-msg? js/Error #"pool terminated" (.claim pool)))))

(deftest ^:async worker-out-of-range
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))]
    (try
      (is (thrown? js/Error (.worker pool -1)))
      (is (thrown? js/Error (.worker pool 2)))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async unknown-module-key
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 1))]
    (try
      (is (thrown? js/Error (.-nope (.worker pool 0))))
      (is (thrown? js/Error (.-nope (.any pool))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async terminate-rejects-further-calls
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 1))]
    (await ((.-terminate pool)))
    (is (thrown-with-msg? js/Error #"pool terminated" (.worker pool 0)))
    (is (thrown-with-msg? js/Error #"pool terminated" (.any pool)))))

(deftest ^:async any-deferred-pick-distributes
  ;; Two any() proxies obtained before either is invoked must each pick the
  ;; least-loaded worker independently: the pick is deferred to the terminal
  ;; call, not fixed when any() is called. Regression for the pick/increment
  ;; decoupling that used to route both to worker 0.
  (let [pool (await
              (make-pool
               #js {:counter #js {:module counter-url :init #js {:start 0}}}
               2))]
    (try
      (let [a (.any pool)
            b (.any pool)]
        (await (js/Promise.all #js [(.inc (.-counter a))
                                    (.inc (.-counter b))])))
      (is (= 1 (await (.get (.-counter (.worker pool 0))))))
      (is (= 1 (await (.get (.-counter (.worker pool 1))))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async any-intermediate-await-does-not-hang
  ;; Awaiting an intermediate any() proxy (before a method is called) must
  ;; not hang or leak: the proxy must not masquerade as a thenable. Race the
  ;; await against a short timer; the await must win.
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))]
    (try
      (let [timer (new js/Promise
                       (fn [resolve _]
                         (js/setTimeout (fn [] (resolve "timeout")) 500)))
            got   (await (js/Promise.race
                          #js [(js/Promise.resolve (.-echo (.any pool)))
                               timer]))]
        (is (not= "timeout" got)
            "awaiting an intermediate any() proxy resolved without hanging"))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async create-rejects-nonpositive-size
  (let [rejected (try
                   (await
                    (.create WorkerPool
                             #js {:bootstrap bootstrap-url
                                  :size 0
                                  :handlers #js {:echo #js {:module echo-url}}}))
                   false
                   (catch :default _ true))]
    (is rejected "WorkerPool.create with size 0 should reject")))

(deftest ^:async terminate-is-idempotent-shared-promise
  ;; A second terminate() returns the same in-flight teardown promise rather
  ;; than resolving early, so a caller awaiting it waits for real completion.
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))
        t1   ((.-terminate pool))
        t2   ((.-terminate pool))]
    (is (identical? t1 t2)
        "repeat terminate() returns the same in-flight promise")
    (await (js/Promise.all #js [t1 t2]))
    (is true "both settled without throwing")))

(deftest ^:async idempotent-init
  (let [opts #js {:bootstrap bootstrap-url
                  :size 1
                  :handlers #js {:t #js {:module init-tracker-url
                                         :init #js {:k "v"}}}}
        a (await (.create WorkerPool opts))]
    (try
      (is (= #js [#js {:k "v"}]
             (await (.calls (.-t (.worker a 0))))))
      (finally
        (await ((.-terminate a)))))
    (let [b (await (.create WorkerPool opts))]
      (try
        (is (= #js [#js {:k "v"}]
               (await (.calls (.-t (.worker b 0))))))
        (finally
          (await ((.-terminate b))))))))

(deftest ^:async create-rejects-when-handlers-empty
  ;; cljs.test doesn't bundle an "async-rejects" macro; reify via
  ;; try/catch around the awaited call and assert the catch branch ran.
  (let [rejected (try
                   (await
                    (.create WorkerPool
                             #js {:bootstrap bootstrap-url
                                  :size 1
                                  :handlers #js {}}))
                   false
                   (catch :default _ true))]
    (is rejected "WorkerPool.create with empty handlers should reject")))

(deftest ^:async destroy-runs-before-worker-term
  ;; Per-worker destroy is hard to observe from the main thread because
  ;; terminate() drops the Comlink RPC channel. The fixture's destroy
  ;; appends one JSON line to logPath capturing both init args (proof init
  ;; ran) and destroy args (proof the spec.destroy payload threaded
  ;; through). After terminate, read the file and assert one line per
  ;; worker with the correct payloads.
  (let [log-path (join (tmpdir)
                       (str "worker-router-destroy-"
                            (.-pid js/process) "-" (.now js/Date) ".jsonl"))]
    (try
      (let [pool (await
                  (.create WorkerPool
                           #js {:bootstrap bootstrap-url
                                :size 3
                                :handlers
                                #js {:tracker
                                     #js {:module destroy-tracker-url
                                          :init #js {:tag "init-payload"}
                                          :destroy #js {:logPath log-path
                                                        :tag "destroy-payload"}}}}))]
        (loop [i 0]
          (when (< i 3)
            (is (= #js {:tag "init-payload"}
                   (await (.snapshotInit (.-tracker (.worker pool i))))))
            (recur (inc i))))
        (await ((.-terminate pool)))
        (let [lines (.split (.trim (readFileSync log-path "utf8")) "\n")]
          (is (= 3 (.-length lines)) "destroy fires once per worker")
          (.forEach lines
                    (fn [line]
                      (let [parsed (.parse js/JSON line)]
                        (is (= #js {:tag "init-payload"} (.-init parsed)))
                        (is (= #js {:logPath log-path :tag "destroy-payload"}
                               (.-destroyArgs parsed)))
                        (is (number? (.-pid parsed))))))))
      (finally
        (try (rmSync log-path #js {:force true})
             (catch :default _ nil))))))

(deftest ^:async destroy-is-optional
  ;; The echo handler exports no top-level destroy. terminate() walks the
  ;; coordinator, sees an empty destroy list, and tears down the worker
  ;; handle anyway. No throw, no hang.
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 2))]
    (await (.echo (.-echo (.worker pool 0)) "hi"))
    (await ((.-terminate pool)))
    (is true "terminate completed without throwing")))

(deftest ^:async create-rejects-on-bad-handler-module
  ;; A handler module that fails to import must reject create() promptly
  ;; with the real cause -- not crash the host process via an unhandled
  ;; worker 'error' event, and not sit out the 5-minute bootstrap timeout.
  ;; This test completing at all is the no-crash assertion.
  (let [err (try
              (await
               (.create WorkerPool
                        #js {:size 2
                             :bootstrap bootstrap-url
                             :handlers #js {:bad #js {:module "file:///nonexistent-worker-router-test.mjs"}}}))
              nil
              (catch :default e e))]
    (is (some? err) "create() with a bad handler module should reject")
    (is (.includes (.-message err) "bootstrap failed")
        (str "rejection should carry the bootstrap failure, got: "
             (some-> err .-message)))))

(deftest ^:async stale-proxy-calls-reject-after-terminate
  ;; Proxies obtained before terminate() must reject at their next call,
  ;; not dispatch into a torn-down worker (a Comlink call that never
  ;; settles -- the caller would hang silently).
  (let [pool      (await (make-pool #js {:echo #js {:module echo-url}} 2))
        stale-any (.any pool)
        stale-w   (.worker pool 0)]
    (await ((.-terminate pool)))
    (let [e1 (try (await (.echo (.-echo stale-any) "x")) nil (catch :default e e))
          e2 (try (await (.echo (.-echo stale-w) "x")) nil (catch :default e e))]
      (is (some? e1) "stale any() call should reject")
      (is (.includes (.-message e1) "pool terminated"))
      (is (some? e2) "stale worker(i) call should reject")
      (is (.includes (.-message e2) "pool terminated")))))

(deftest ^:async worker-proxy-is-not-a-thenable
  ;; Promise assimilation probes `then` on await / async return. The
  ;; dispatching proxy must return undefined for it rather than throwing
  ;; the unknown-module-key error.
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 1))]
    (try
      (is (some? (await (js/Promise.resolve (.worker pool 0))))
          "awaiting a worker(i) proxy resolves to the proxy itself")
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async dead-worker-rejects-calls-and-terminate-completes
  ;; A worker that exits marks its record dead: further calls to it reject
  ;; instead of hanging, and terminate() skips its coordinator shutdown RPC
  ;; (which would never settle) so teardown still completes.
  (let [pool (await
              (.create WorkerPool
                       #js {:size 1
                            :bootstrap bootstrap-url
                            :handlers #js {:d #js {:module exit-url}}}))]
    (is (= "pong" (await (.ping (.-d (.worker pool 0))))))
    (is (= "dying" (await (.die (.-d (.worker pool 0))))))
    (await (new js/Promise (fn [resolve] (js/setTimeout resolve 200))))
    (let [err (try (await (.ping (.-d (.worker pool 0)))) nil (catch :default e e))]
      (is (some? err) "call to a dead worker should reject")
      (is (.includes (.-message err) "is dead")))
    (let [outcome (await
                   (js/Promise.race
                    #js [(.then ((.-terminate pool)) (fn [_] "terminated"))
                         (new js/Promise
                              (fn [resolve]
                                (js/setTimeout (fn [] (resolve "hung")) 3000)))]))]
      (is (= "terminated" outcome)
          "terminate() must complete despite the dead worker"))))

(deftest ^:async in-flight-call-rejects-when-terminate-races
  ;; Contract: a call already dispatched when terminate() is invoked must
  ;; settle (reject with "pool terminated"), not hang forever. terminate()
  ;; tears the worker down, so the underlying Comlink call never settles on
  ;; its own; the pool must reject the caller instead of leaving it wedged.
  ;; Race the in-flight call against a 2s timer; a timer win means it hung.
  (let [pool     (await (make-pool #js {:s #js {:module slow-url}} 1))
        inflight (.slow (.-s (.worker pool 0)) 1000)]
    (await (new js/Promise (fn [resolve] (js/setTimeout resolve 50))))
    (let [term    ((.-terminate pool))
          outcome (await
                   (js/Promise.race
                    #js [(.then inflight
                                (fn [v] (str "resolved:" v))
                                (fn [e] (str "rejected:" (.-message e))))
                         (new js/Promise
                              (fn [resolve]
                                (js/setTimeout (fn [] (resolve "HUNG")) 2000)))]))]
      (await term)
      (is (not= "HUNG" outcome)
          "in-flight call during terminate must settle, not hang")
      (is (.startsWith outcome "rejected:")
          (str "in-flight call should reject when terminate races it, got: "
               outcome))
      (is (.includes outcome "terminated")
          (str "rejection should mention pool terminated, got: " outcome)))))

(deftest ^:async in-flight-call-rejects-when-worker-crashes
  ;; Contract: a call in flight when its worker crashes mid-call must reject
  ;; rather than hang. The README promises calls to a crashed worker "reject
  ;; rather than hang"; that must hold for a call already dispatched at the
  ;; moment of the crash, not only for calls started afterward. The fixture
  ;; never resolves the call and hard-exits ~80ms in.
  (let [pool (await (make-pool #js {:c #js {:module crash-inflight-url}} 1))]
    (is (= "pong" (await (.ping (.-c (.worker pool 0))))))
    (let [inflight (.slowThenDie (.-c (.worker pool 0)) 80)
          outcome  (await
                    (js/Promise.race
                     #js [(.then inflight
                                 (fn [v] (str "resolved:" v))
                                 (fn [e] (str "rejected:" (.-message e))))
                          (new js/Promise
                               (fn [resolve]
                                 (js/setTimeout (fn [] (resolve "HUNG")) 3000)))]))]
      (is (not= "HUNG" outcome)
          "in-flight call when its worker crashes must settle, not hang")
      (is (.startsWith outcome "rejected:")
          (str "in-flight call should reject on worker crash, got: " outcome)))
    (await ((.-terminate pool)))))

(deftest ^:async create-window-worker-crash-is-contained
  ;; Contract: a worker that dies (async throw) in the window between posting
  ;; its ready handshake and the pool attaching its permanent liveness
  ;; listeners must be contained. It must not (a) crash the host via an
  ;; unhandled 'error' event, (b) go unflagged so calls to it hang forever,
  ;; or (c) stall terminate(). The fixture forces one worker to ready fast and
  ;; throw 150ms later while a sibling is still booting (~500ms), so the crash
  ;; lands squarely in that window.
  (let [marker (join (tmpdir)
                     (str "wr-window-" (.-pid js/process) "-" (.now js/Date)))
        _      (try (rmSync marker #js {:force true}) (catch :default _ nil))
        _      (set! (.-WR_WINDOW_MARKER (.-env js/process)) marker)
        before (.-length host-uncaught)
        settle (fn [p label]
                 (js/Promise.race
                  #js [(.then p
                              (fn [v] (str "ok:" v))
                              (fn [e] (str "rej:" (.-message e))))
                       (new js/Promise
                            (fn [resolve]
                              (js/setTimeout
                               (fn [] (resolve (str "HUNG:" label))) 1500)))]))]
    (try
      (let [pool (await (make-pool #js {:h #js {:module window-crash-url}} 2))
            w0   (await (settle (.ping (.-h (.worker pool 0))) "w0"))
            w1   (await (settle (.ping (.-h (.worker pool 1))) "w1"))
            tm   (await (settle ((.-terminate pool)) "terminate"))]
        (is (not (.startsWith w0 "HUNG")) (str "worker 0 call hung: " w0))
        (is (not (.startsWith w1 "HUNG")) (str "worker 1 call hung: " w1))
        (is (not (.startsWith tm "HUNG")) (str "terminate hung: " tm)))
      (finally
        (try (rmSync marker #js {:force true}) (catch :default _ nil))))
    (is (= before (.-length host-uncaught))
        (str "host uncaughtException leaked from a bootstrap-window crash: "
             (.join (.slice host-uncaught before) " | ")))))

(deftest ^:async unexpected-worker-exit-is-logged
  ;; Observability contract: when a worker dies unexpectedly (a non-zero exit
  ;; the pool did not initiate), the pool logs it via console.error, so a
  ;; downstream user sees that a worker went away rather than only meeting a
  ;; later "is dead" rejection. A clean terminate() must NOT emit such a log
  ;; (its exits are expected), or every shutdown would cry wolf.
  (let [orig     (.-error js/console)
        captured (js/Array.)]
    (set! (.-error js/console)
          (fn [& args] (.push captured (.join (js/Array.from args) " "))))
    (try
      (let [pool (await
                  (.create WorkerPool
                           #js {:size 1
                                :bootstrap bootstrap-url
                                :handlers #js {:d #js {:module exit-url}}}))]
        (is (= "dying" (await (.die (.-d (.worker pool 0))))))
        (await (new js/Promise (fn [resolve] (js/setTimeout resolve 250))))
        (is (some (fn [m] (and (.includes m "worker") (.includes m "exited")))
                  captured)
            (str "unexpected worker exit should be logged; captured: "
                 (.join captured " || ")))
        (await ((.-terminate pool))))
      (let [before (.-length captured)
            pool   (await (make-pool #js {:echo #js {:module echo-url}} 2))]
        (await (.echo (.-echo (.worker pool 0)) "hi"))
        (await ((.-terminate pool)))
        (await (new js/Promise (fn [resolve] (js/setTimeout resolve 150))))
        (is (not (some (fn [m] (.includes m "exited unexpectedly"))
                       (.slice captured before)))
            (str "clean terminate must not log unexpected exits; captured: "
                 (.join (.slice captured before) " || "))))
      (finally
        (set! (.-error js/console) orig)))))

(deftest ^:async bootstrap-timeout-is-configurable
  ;; The silent bootstrap stays alive but never posts ready, so only the
  ;; timeout can settle create(). With bootstrapTimeoutMs at 300 the
  ;; rejection must arrive far inside the 300s default.
  (let [t0  (.now js/Date)
        err (try
              (await
               (.create WorkerPool
                        #js {:size 1
                             :bootstrap silent-bootstrap-url
                             :bootstrapTimeoutMs 300
                             :handlers #js {:echo #js {:module echo-url}}}))
              nil
              (catch :default e e))]
    (is (some? err) "create() should reject on bootstrap timeout")
    (is (.includes (.-message err) "bootstrap timeout"))
    (is (< (- (.now js/Date) t0) 5000)
        "rejection should honor the configured timeout, not the default")))

(deftest ^:async shutdown-timeout-is-configurable
  ;; The fixture's destroy never settles; terminate() must give up on it
  ;; after shutdownTimeoutMs and still tear the worker down.
  (let [pool (await
              (.create WorkerPool
                       #js {:size 1
                            :bootstrap bootstrap-url
                            :shutdownTimeoutMs 300
                            :handlers #js {:h #js {:module hang-destroy-url}}}))]
    (is (= "pong" (await (.ping (.-h (.worker pool 0))))))
    (let [t0      (.now js/Date)
          outcome (await
                   (js/Promise.race
                    #js [(.then ((.-terminate pool)) (fn [_] "terminated"))
                         (new js/Promise
                              (fn [resolve]
                                (js/setTimeout (fn [] (resolve "hung")) 4000)))]))]
      (is (= "terminated" outcome))
      (is (< (- (.now js/Date) t0) 4000)
          "terminate should give up on the hung destroy promptly"))))

(deftest ^:async proxies-tolerate-probe-keys
  ;; JSON.stringify, string coercion, and promise assimilation probe
  ;; toJSON/valueOf/toString/constructor/then as string keys. Both proxy
  ;; flavors must delegate these to the raw target: worker(i) used to
  ;; throw unknown-module-key on them, and any() used to record toJSON
  ;; as a call path, turning stringify into a live RPC. Typo strictness
  ;; must survive for every other key.
  (let [pool (await (make-pool #js {:echo #js {:module echo-url}} 1))]
    (try
      (is (= "{}" (js/JSON.stringify (.worker pool 0))))
      (is (= "{}" (js/JSON.stringify #js {:p (.any pool)})))
      (is (= "[object Object]" (str (.worker pool 0))))
      (is (string? (str (.any pool))))
      (is (thrown-with-msg? js/Error #"unknown module key"
                            (.-nope (.worker pool 0))))
      (finally
        (await ((.-terminate pool)))))))

(deftest ^:async comlink-url-option-is-honored
  ;; A valid explicit comlinkUrl must work end to end, and a bogus one
  ;; must reject create() -- proof the option reaches the worker instead
  ;; of the worker falling back to its own bare "comlink" resolution.
  (let [good (await
              (.create WorkerPool
                       #js {:size 1
                            :bootstrap bootstrap-url
                            :comlinkUrl (.resolve js/import.meta "comlink")
                            :handlers #js {:echo #js {:module echo-url}}}))]
    (try
      (is (= "hi" (await (.echo (.-echo (.worker good 0)) "hi"))))
      (finally
        (await ((.-terminate good))))))
  (let [err (try
              (await
               (.create WorkerPool
                        #js {:size 1
                             :bootstrap bootstrap-url
                             :comlinkUrl "file:///nonexistent-comlink.mjs"
                             :handlers #js {:echo #js {:module echo-url}}}))
              nil
              (catch :default e e))]
    (is (some? err) "bogus comlinkUrl should reject create()")
    (is (.includes (.-message err) "bootstrap failed"))))

(deftest detect-runtime-returns-node-under-node-test
  (is (= "node" (detectRuntime))))

(deftest ^:async spawn-node-creates-worker-that-can-post
  (let [handle    (await (spawn noop-bootstrap-url))
        raw       (.-raw handle)
        endpoint  (.-endpoint handle)
        terminate (.-terminate handle)
        msgs      (await (once raw "message"))
        msg       (aget msgs 0)]
    (is (= #js {:type "worker-router/ready"} msg))
    (await (terminate))
    (is (some? endpoint))))

;; Tests dist/worker-bootstrap.mjs directly via raw Worker + Comlink.wrap,
;; bypassing WorkerPool entirely.

(defn ^:async wait-for-ready [raw]
  (loop []
    (let [msgs (await (once raw "message"))
          msg  (aget msgs 0)]
      (if (= "worker-router/ready" (and msg (.-type msg)))
        nil
        (recur)))))

(deftest ^:async bootstrap-imports-handlers-exposes-them-signals-ready
  (let [raw   (new Worker bootstrap-url-obj)
        ready (wait-for-ready raw)]
    (.postMessage raw #js {:type "worker-router/bootstrap"
                           :handlers #js {:echo #js {:module echo-url}
                                          :f    #js {:module factory-url
                                                     :init #js {:prefix "hi"}}}})
    (await ready)
    (let [proxy (Comlink/wrap (nodeEndpoint raw))]
      (is (= "ping" (await (.echo (.-echo proxy) "ping"))))
      (is (= 6 (await (.double (.-nested (.-echo proxy)) 3))))
      (is (= "hi:x" (await (.greet (.-f proxy) "x")))))
    (await (.terminate raw))))

;; The spawn() path lazily imports node:worker_threads inside an async
;; function so dist/index.mjs evaluates cleanly in a browser. A top-level
;; static node:* import breaks browser eval, and node can't catch that
;; regression. Serve the repo over http, load dist/index.mjs in headless
;; chromium via Playwright, and assert (1) the module evaluates without
;; throwing and (2) a real Web Worker pool round-trips an echo call and
;; terminates. Same-origin module workers need no special headers, so the
;; bare http server suffices.

(def browser-eval-mime
  #js {".html" "text/html"
       ".js"   "application/javascript"
       ".mjs"  "application/javascript"
       ".css"  "text/css"
       ".json" "application/json"})

(def browser-eval-root
  (fileURLToPath (new js/URL "../../" js/import.meta.url)))

(deftest ^:async browser-eval-module-evaluates-in-chromium
  (let [server (createServer
                ^:async (fn [req res]
                          (try
                            (let [url       (new js/URL (.-url req)
                                                 (str "http://" (.-host (.-headers req))))
                                  pathname  (.-pathname url)
                                  p         (if (= pathname "/")
                                              "/test/fixtures/browser-eval.html"
                                              pathname)
                                  file-path (join browser-eval-root p)
                                  data      (await (readFile file-path))
                                  ext       (extname file-path)]
                              (.setHeader res "Content-Type"
                                          (or (aget browser-eval-mime ext)
                                              "application/octet-stream"))
                              (.end res data))
                            (catch :default _
                              (set! (.-statusCode res) 404)
                              (.end res (str "not found: " (.-url req)))))))]
    (await (js/Promise. (fn [resolve]
                             (.listen server 0 "127.0.0.1" resolve))))
    (let [port    (.-port (.address server))
          browser (await (.launch chromium #js {:headless true}))]
      (try
        (let [page   (await (.newPage browser))
              errors (js/Array.)]
          (.on page "pageerror" (fn [err] (.push errors (.-message err))))
          (.on page "console"
               (fn [msg]
                 (when (= "error" (.type msg))
                   (.push errors (str "console.error: " (.text msg))))))
          (await (.goto page (str "http://127.0.0.1:" port "/")
                           #js {:waitUntil "load"}))
          ;; __poolDone flips after both probes: module evaluation and the
          ;; real worker-pool round-trip (spawn 2 workers, echo, terminate).
          (await (.waitForFunction page
                                      "window.__poolDone === true"
                                      nil #js {:timeout 20000}))
          (let [loaded       (await (.evaluate page "window.__loaded"))
                error        (await (.evaluate page "window.__error"))
                exports      (await (.evaluate page "window.__exports"))
                runtime      (await (.evaluate page "window.__runtime"))
                pool-error   (await (.evaluate page "window.__poolError"))
                pool-echo    (await (.evaluate page "window.__poolEcho"))
                pool-runtime (await (.evaluate page "window.__poolRuntime"))]
            (is (= true loaded)
                (str "module failed to load in browser: " error
                     " (page errors: " (.join errors " | ") ")"))
            (is (= #js ["WorkerPool" "detectRuntime" "spawn"] (.sort exports))
                (str "unexpected exports: " (.join exports ", ")))
            (is (= "browser" runtime)
                (str "detectRuntime() in browser returned " runtime))
            (is (nil? pool-error)
                (str "browser pool round-trip failed: " pool-error))
            (is (= "browser-ok" pool-echo)
                "pool.any().echo.echo round-trip in chromium")
            (is (= "browser" pool-runtime))
            (is (= 0 (.-length errors))
                (str "unexpected page errors: " (.join errors " | ")))))
        (finally
          (await (.close browser))
          (await (js/Promise. (fn [resolve] (.close server resolve)))))))))

;; Run on module load. node test/cljc/pool_test.mjs imports this file;
;; deftest forms above register at top level; run-tests then iterates
;; the registry and awaits any Promise each test returns. Exit non-zero
;; if any test failed or errored so bb test:cljs reports a real failure.
(.then (t/run-tests)
       (fn [results]
         (let [fail (or (get results "fail") 0)
               err  (or (get results "error") 0)]
           (when (pos? (+ fail err))
             (.exit js/process 1)))))
