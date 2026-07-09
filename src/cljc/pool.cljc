;; Copyright 2026 Will Cohen
;; SPDX-License-Identifier: Apache-2.0
;;
;; The pool implementation. Squint-compiled to dist/index.mjs as the
;; package's main export.

(ns pool
  (:require ["comlink" :as Comlink]))

(def ^:private DEFAULT-SIZE-FALLBACK 4)
;; Defaults for the two PoolOptions timeouts. The bootstrap default is
;; generous because handler init can legitimately take a while (large
;; WASM loads); error/exit listeners catch real failures fast, so this
;; only bites a worker that hangs without erroring. The shutdown cap
;; exists because Comlink never settles a call whose far endpoint died;
;; without it one wedged worker would hang terminate() forever.
(def ^:private DEFAULT-BOOTSTRAP-TIMEOUT-MS 300000)
(def ^:private DEFAULT-SHUTDOWN-TIMEOUT-MS 5000)
;; Wire value the bootstrap exposes on every worker for the pool to
;; invoke at terminate-time. Must match the constant in
;; src/worker-bootstrap.mjs.
(def ^:private WORKER-ROUTER-COORDINATOR-KEY "__worker_router__")

;; Property names that promise machinery, JSON.stringify, and string
;; coercion probe on arbitrary objects. Both dispatch proxies delegate
;; these to the raw target instead of throwing (worker(i)) or recording
;; a call path (any()), so proxies are safe to await, log, and
;; serialize. None is a plausible module key: each would shadow an
;; Object.prototype member. Delegation (not undefined) matters for
;; toString/valueOf -- returning undefined would break string coercion
;; with a TypeError instead of using the prototype implementations.
(def ^:private PROBE-KEYS
  (new js/Set #js ["then" "toJSON" "valueOf" "toString" "constructor"]))

(defn ^:private detect-runtime []
  (let [mp (.-process js/globalThis)]
    (if (and mp
             (.-versions mp)
             (string? (.-node (.-versions mp))))
      "node"
      "browser")))

(defn detectRuntime []
  ;; camelCase public alias mirroring the TS surface.
  (detect-runtime))

(defn ^:private resolve-size [size]
  (if (= size "auto")
    (let [hc (some-> js/globalThis .-navigator .-hardwareConcurrency)]
      (if (and (number? hc) (pos? hc)) hc DEFAULT-SIZE-FALLBACK))
    size))

(defn ^:async spawn
  "Spawn one worker for `bootstrap-url`. Returns a handle
   #js {:raw :endpoint :terminate}."
  [bootstrap-url]
  (if (= (detect-runtime) "node")
    (let [wt        (await (js/import "node:worker_threads"))
          NW        (.-Worker wt)
          ne-mod    (await (js/import "comlink/dist/esm/node-adapter.mjs"))
          ne        (.-default ne-mod)
          raw       (new NW (new js/URL bootstrap-url))
          endpoint  (ne raw)]
      #js {:raw raw
           :endpoint endpoint
           :terminate (fn [] (.terminate raw))})
    (let [raw (new js/Worker bootstrap-url #js {:type "module"})]
      #js {:raw raw
           :endpoint raw
           ;; Browser Worker.terminate() returns undefined; wrap so the
           ;; handle's terminate is a promise on both platforms.
           :terminate (fn [] (js/Promise.resolve (.terminate raw)))})))

(defn ^:async ^:private await-ready
  "Resolve on the first 'worker-router/ready' message from `handle.raw`;
   reject on a 'worker-router/error' message (the bootstrap caught a
   handler load failure), on a worker 'error'/'exit' event (the worker
   died before it could report), or on timeout. The finally removes every
   listener and clears the timer on all paths, so a failed worker leaves
   nothing behind."
  [handle timeout-ms]
  (let [raw         (.-raw handle)
        timeout-id  (volatile! nil)
        cleanup     (volatile! (fn []))
        timer       (new js/Promise
                         (fn [_ reject]
                           (let [tid (js/setTimeout
                                      (fn []
                                        (reject (js/Error.
                                                 "worker-router: bootstrap timeout")))
                                      timeout-ms)]
                             (vreset! timeout-id tid)
                             (when (and tid (.-unref tid))
                               (.unref tid)))))
        ready       (new js/Promise
                         (fn [resolve reject]
                           (let [on-msg (fn [t message]
                                          (cond
                                            (= t "worker-router/ready")
                                            (resolve)

                                            (= t "worker-router/error")
                                            (reject (js/Error.
                                                     (str "worker-router: worker bootstrap failed: "
                                                          message)))))]
                             (cond
                               (and (.-on raw) (.-removeListener raw))
                               (let [msg-listener  (fn [m]
                                                     (on-msg (some-> m .-type)
                                                             (some-> m .-message)))
                                     err-listener  (fn [err] (reject err))
                                     exit-listener (fn [code]
                                                     (reject
                                                      (js/Error.
                                                       (str "worker-router: worker exited during bootstrap (code "
                                                            code ")"))))]
                                 (vreset! cleanup
                                          (fn []
                                            (.removeListener raw "message" msg-listener)
                                            (.removeListener raw "error" err-listener)
                                            (.removeListener raw "exit" exit-listener)))
                                 (.on raw "message" msg-listener)
                                 ;; An unhandled 'error' event on a node Worker is an
                                 ;; uncaughtException in the MAIN thread. Listening here
                                 ;; turns a broken bootstrap into a create() rejection
                                 ;; instead of a host-process crash.
                                 (.on raw "error" err-listener)
                                 (.on raw "exit" exit-listener))

                               (.-addEventListener raw)
                               (let [msg-listener (fn [ev]
                                                    (on-msg (some-> ev .-data .-type)
                                                            (some-> ev .-data .-message)))
                                     err-listener (fn [ev]
                                                    (reject
                                                     (js/Error.
                                                      (str "worker-router: worker error during bootstrap: "
                                                           (or (.-message ev) "unknown")))))]
                                 (vreset! cleanup
                                          (fn []
                                            (.removeEventListener raw "message" msg-listener)
                                            (.removeEventListener raw "error" err-listener)))
                                 (.addEventListener raw "message" msg-listener)
                                 (.addEventListener raw "error" err-listener))))))]
    (try
      (await (js/Promise.race #js [ready timer]))
      (finally
        (@cleanup)
        (when-let [tid @timeout-id]
          (js/clearTimeout tid))))))

(defn ^:private count-call
  "Increment `worker.pending`, run `invoke` (a 0-arg fn performing the
   underlying Comlink call), and decrement on settle. A synchronous throw
   from `invoke` is funneled into a rejected promise so the decrement still
   runs and the caller still observes the error. The decrement `.finally`
   chain carries a terminal `.catch` so its view of a rejection isn't
   reported as unhandledRejection; the returned promise keeps the rejection
   for the caller.

   Rejects up front when the pool is terminated or the worker has died:
   dispatching into a gone worker is a Comlink call that never settles, so
   without this guard a stale proxy hangs its caller silently.

   For a call that WAS dispatched while the worker was live and is still in
   flight when the worker later terminates or crashes, the underlying Comlink
   call never settles either. Racing it against the worker's `signal` (a
   promise that rejects on terminate or death) rejects the caller instead of
   wedging it. The pending decrement hangs off the raced promise, so it fires
   exactly once whichever side wins; the losing Comlink promise keeps a
   reaction attached (via Promise.race) so its later settlement isn't an
   unhandledRejection."
  [worker invoke]
  (cond
    (.-terminated worker)
    (js/Promise.reject (js/Error. "worker-router: pool terminated"))

    (.-dead worker)
    (js/Promise.reject (js/Error. (str "worker-router: worker "
                                       (.-index worker) " is dead")))

    :else
    (do
      (set! (.-pending worker) (inc (.-pending worker)))
      (let [p     (try
                    (js/Promise.resolve (invoke))
                    (catch :default e (js/Promise.reject e)))
            raced (js/Promise.race #js [p (.-signal worker)])]
        (-> raced
            (.finally (fn [] (set! (.-pending worker) (dec (.-pending worker)))))
            (.catch (fn [_])))
        raced))))

(defn ^:private wrap-for-counting
  "Recursive Proxy: every invocation at any depth beneath a module key is
   counted via count-call. Symbol property access passes through unchanged
   so Comlink internals (releaseProxy, finalizer) aren't counted."
  [worker inner]
  (cond
    ;; squint nil? compiles to `== null`, which covers undefined too.
    (nil? inner) inner
    ;; Wrap functions and reference types (plain objects, class
    ;; instances, arrays, Comlink Remote proxies) so invocations at any
    ;; depth are counted; let number/string/boolean primitives pass
    ;; through untouched. nil and undefined are handled above.
    (or (fn? inner)
        (and (not (number? inner))
             (not (string? inner))
             (not (boolean? inner))))
    (new js/Proxy inner
         #js {:get (fn [target prop receiver]
                     ;; string? on the Proxy `get` trap's prop arg is true
                     ;; for string property keys and false for symbol keys.
                     ;; Symbol keys (Comlink.releaseProxy / .finalizer)
                     ;; pass through without re-wrapping.
                     (if (string? prop)
                       (wrap-for-counting worker
                                          (js/Reflect.get target prop receiver))
                       (js/Reflect.get target prop receiver)))
              :apply (fn [target this-arg args]
                       (count-call worker
                                   (fn [] (js/Reflect.apply target this-arg args))))})
    :else inner))

(defn ^:private pick-least-loaded [all-records]
  ;; Strict `<` (not `<=`) so ties break to the lowest-index worker.
  ;; argmin(pending + claims): both `any()` and `claim()` consume picks
  ;; here, so transient (pending) and durable (claims) load feed back
  ;; into each other through the single sum. Dead workers are skipped
  ;; while any live one remains; if all are dead the pick falls through
  ;; to the full set and count-call rejects with a useful error.
  (let [alive       (.filter all-records (fn [r] (not (.-dead r))))
        records-arr (if (pos? (.-length alive)) alive all-records)
        n (alength records-arr)
        w0 (aget records-arr 0)]
    (loop [i 1
           best w0
           best-load (+ (.-pending w0) (.-claims w0))]
      (if (< i n)
        (let [w (aget records-arr i)
              load (+ (.-pending w) (.-claims w))]
          (if (< load best-load)
            (recur (inc i) w load)
            (recur (inc i) best best-load)))
        best))))

(defn ^:private dispatching-proxy [worker registered-modules]
  (new js/Proxy
       #js {}
       #js {:get (fn [target key receiver]
                   (cond
                     (not (string? key)) js/undefined
                     (.has PROBE-KEYS key) (js/Reflect.get target key receiver)
                     (not (.has registered-modules key))
                     (throw (js/Error.
                             (str "worker-router: unknown module key \""
                                  key "\"")))
                     :else
                     (let [inner (aget (.-proxy worker) key)]
                       (wrap-for-counting worker inner))))}))

(defn ^:private any-proxy
  "Least-loaded dispatch proxy whose worker pick is deferred to the terminal
   call: pick-least-loaded runs inside the apply trap, not when any() is
   called, so the load it reads is current. Two any() proxies obtained before
   either is invoked each pick independently at their own call moment. The
   proxy records the module/method access path; the first hop is validated
   against registered-modules eagerly so an unknown key throws at access time
   (matching the affinity proxy). At apply it picks the worker, walks that
   worker's Comlink proxy along the recorded path, and counts the call."
  [records-arr registered-modules path]
  (new js/Proxy (fn [])
       #js {:get (fn [target key receiver]
                   (cond
                     (not (string? key)) js/undefined
                     ;; Never record probe keys as path segments. `then` would
                     ;; make the proxy look like a thenable (awaiting it hangs);
                     ;; `toJSON` would turn JSON.stringify into a live RPC to a
                     ;; method that doesn't exist. Delegate to the raw target.
                     (.has PROBE-KEYS key) (js/Reflect.get target key receiver)
                     :else
                     (do
                       (when (and (zero? (.-length path))
                                  (not (.has registered-modules key)))
                         (throw (js/Error.
                                 (str "worker-router: unknown module key \""
                                      key "\""))))
                       (any-proxy records-arr registered-modules
                                  (.concat path #js [key])))))
            :apply (fn [_target _this args]
                     (when (zero? (.-length path))
                       (throw (js/Error.
                               "worker-router: any() proxy invoked before selecting a module method")))
                     (let [worker   (pick-least-loaded records-arr)
                           last-idx (dec (.-length path))
                           parent   (.reduce (.slice path 0 last-idx)
                                             (fn [o k] (aget o k))
                                             (.-proxy worker))
                           f        (aget parent (aget path last-idx))]
                       (count-call worker
                                   (fn [] (js/Reflect.apply f parent args)))))}))

(defn ^:private resolve-within
  "Race `p` against a timer that RESOLVES (undefined) after `ms`. Used for
   terminate's shutdown phase: giving up on a wedged worker must not fail
   the teardown, so the timer resolves rather than rejects. The timer is
   cleared (or unref'd where supported) so it never holds the event loop."
  [p ms]
  (new js/Promise
       (fn [resolve reject]
         (let [tid (js/setTimeout (fn [] (resolve js/undefined)) ms)]
           (when (and tid (.-unref tid))
             (.unref tid))
           (-> (js/Promise.resolve p)
               (.then (fn [v] (js/clearTimeout tid) (resolve v))
                      (fn [e] (js/clearTimeout tid) (reject e))))))))

(defn ^:private watch-liveness
  "Attach for-the-worker's-lifetime error/exit listeners that flag the
   record dead. On node the 'error' listener is also load-bearing for the
   host: an unhandled 'error' event on a Worker is an uncaughtException in
   the main thread, so a post-bootstrap worker crash would otherwise take
   the host process down."
  [record]
  (let [raw  (.-raw (.-handle record))
        mark (fn []
               (set! (.-dead record) true)
               ;; Reject any call still in flight on this worker. Rejecting an
               ;; already-settled signal (e.g. exit after error) is a no-op.
               ((.-signalReject record)
                (js/Error. (str "worker-router: worker "
                                (.-index record) " died"))))]
    (cond
      (and (.-on raw) (.-removeListener raw))
      (do
        (.on raw "error"
             (fn [err]
               (mark)
               (js/console.error "worker-router: worker" (.-index record)
                                 "error:" err)))
        (.on raw "exit"
             (fn [code]
               (mark)
               ;; A non-zero exit the pool did not initiate is a real crash
               ;; (process.exit, OOM-kill, native fault) -- surface it so the
               ;; caller isn't left guessing. Stay quiet when we terminated
               ;; the worker ourselves (terminated flag set before teardown),
               ;; since terminate() produces a non-zero exit by design.
               (when (and (not (.-terminated record))
                          (number? code)
                          (not (zero? code)))
                 (js/console.error "worker-router: worker" (.-index record)
                                   "exited unexpectedly (code" code ")")))))

      (.-addEventListener raw)
      (.addEventListener raw "error"
                         (fn [ev]
                           (mark)
                           (js/console.error "worker-router: worker"
                                             (.-index record) "error:"
                                             (or (.-message ev) ev)))))))

(defn ^:async ^:private create-pool
  "Spawn `opts.size` workers, post the bootstrap message to each, await
   their ready signals, and return a #js {:size :runtime
   :registeredModules :worker :any :claim :terminate} pool object.

   opts: #js {:size (number | 'auto')
              :bootstrap (string URL)
              :handlers  (JS object map of module-key -> {module, init})
              :bootstrapTimeoutMs (optional, default 300000)
              :shutdownTimeoutMs  (optional, default 5000)
              :comlinkUrl (optional; overrides import.meta.resolve)}"
  [opts]
  (let [handlers (.-handlers opts)
        keys-arr (js/Object.keys handlers)
        size     (resolve-size (.-size opts))]
    (when (zero? (.-length keys-arr))
      (throw (js/Error.
              "worker-router: at least one handler must be registered")))
    (when (nil? (.-size opts))
      (throw (js/Error. "worker-router: size is required")))
    (when (or (not (js/Number.isInteger size)) (< size 1))
      (throw (js/Error.
              (str "worker-router: size must be a positive integer, got "
                   size))))
    (let [runtime           (detect-runtime)
          bootstrap         (.-bootstrap opts)
          bootstrap-timeout (or (.-bootstrapTimeoutMs opts)
                                DEFAULT-BOOTSTRAP-TIMEOUT-MS)
          shutdown-timeout  (or (.-shutdownTimeoutMs opts)
                                DEFAULT-SHUTDOWN-TIMEOUT-MS)
          ;; Dedicated module workers don't inherit the page's import map,
          ;; so a worker can't resolve the bare "comlink" specifier itself.
          ;; Resolve it here in the page realm (which honors the import
          ;; map; Node resolves via node_modules) and hand the worker an
          ;; absolute URL to dynamic-import -- the same mechanism the
          ;; handler modules already use via their .module URLs. An
          ;; explicit opts.comlinkUrl bypasses resolution for bundlers
          ;; that mangle or drop import.meta.resolve.
          comlink-url   (or (.-comlinkUrl opts)
                            (.resolve js/import.meta "comlink"))
          bootstrap-msg #js {:type "worker-router/bootstrap"
                             :handlers handlers
                             :comlinkUrl comlink-url}
          spawn-promises (.from js/Array #js {:length size}
                                (fn [_ _i] (spawn bootstrap)))
          spawn-settled  (await (js/Promise.allSettled spawn-promises))
          rejected       (.filter spawn-settled
                                  (fn [s] (= (.-status s) "rejected")))
          handles        (.map (.filter spawn-settled
                                        (fn [s] (= (.-status s) "fulfilled")))
                               (fn [s] (.-value s)))]
      ;; A worker that spawns while a sibling spawn rejects must still be
      ;; torn down, or it leaks: Promise.all would drop the survivors on the
      ;; floor. Gather every fulfilled handle, terminate them, then surface
      ;; the first failure.
      (when (pos? (.-length rejected))
        (await
         (js/Promise.allSettled
          (.map handles (fn [h] ((.-terminate h))))))
        (throw (.-reason (aget rejected 0))))
      (let [records-arr (.map handles
                              (fn [handle idx]
                                ;; `signal` rejects when this worker becomes
                                ;; unusable (pool terminated or worker died);
                                ;; count-call races in-flight calls against it
                                ;; so they reject instead of hanging on a
                                ;; Comlink call that will never settle. The
                                ;; terminal .catch keeps an un-raced rejection
                                ;; (terminate on an idle worker) from surfacing
                                ;; as an unhandledRejection.
                                (let [reject-box (volatile! nil)
                                      signal (new js/Promise
                                                  (fn [_ reject]
                                                    (vreset! reject-box reject)))]
                                  (.catch signal (fn [_]))
                                  #js {:index idx
                                       :handle handle
                                       :proxy (Comlink/wrap (.-endpoint handle))
                                       :pending 0
                                       :claims 0
                                       :dead false
                                       :terminated false
                                       :signal signal
                                       :signalReject @reject-box})))]
        ;; Attach liveness listeners BEFORE the ready handshake so no worker is
        ;; ever without an error/exit listener. A worker that dies in the
        ;; window between posting ready and here would otherwise go unflagged
        ;; (its death unobserved, so calls to it hang and its signal never
        ;; fires) and, on node, its unhandled 'error' event would take the host
        ;; process down. watch-liveness runs for the worker's whole life;
        ;; await-ready adds and removes only its own bootstrap listeners, so
        ;; the two never leave a gap.
        (.forEach records-arr (fn [r] (watch-liveness r)))
        (try
          (doseq [h handles]
            (.postMessage (.-raw h) bootstrap-msg))
          (await
           (js/Promise.all
            (.map handles (fn [h] (await-ready h bootstrap-timeout)))))
          (catch :default err
            (await
             (js/Promise.allSettled
              (.map handles (fn [h] ((.-terminate h))))))
            (throw err)))
        (let [registered-modules (new js/Set keys-arr)
              ;; Holds the in-flight teardown promise. nil until terminate()
              ;; is first called; set synchronously on that call so both
              ;; assert-live (dispatch guard) and repeat terminate() calls
              ;; observe it immediately and share the one teardown.
              terminate-promise  (volatile! nil)
              assert-live (fn []
                            (when @terminate-promise
                              (throw (js/Error. "worker-router: pool terminated"))))]
          #js {:size size
             :runtime runtime
             :registeredModules registered-modules
             :worker
             (fn [index]
               (assert-live)
               (when (or (not (js/Number.isInteger index))
                         (< index 0)
                         (>= index size))
                 (throw (js/RangeError.
                         (str "worker-router: worker index " index
                              " out of range [0, " size ")"))))
               (dispatching-proxy (aget records-arr index)
                                  registered-modules))
             :any
             (fn []
               (assert-live)
               (any-proxy records-arr registered-modules #js []))
             :claim
             (fn []
               (assert-live)
               (let [worker (pick-least-loaded records-arr)]
                 (set! (.-claims worker) (inc (.-claims worker)))
                 (let [released (volatile! false)
                       release  (fn []
                                  (when-not @released
                                    (vreset! released true)
                                    (set! (.-claims worker)
                                          (dec (.-claims worker)))))]
                   #js {:index (.-index worker) :release release})))
             :terminate
             ;; Returns a Promise directly (no inner async/await): squint
             ;; doesn't propagate `^:async` onto anonymous `fn` forms nested
             ;; in `#js {}` value positions, so an `await` here would land in
             ;; a non-async function and fail compile. Observationally
             ;; equivalent for callers that `await pool.terminate()`.
             ;;
             ;; Idempotent: the teardown promise is built once and cached, so
             ;; a second terminate() returns the same in-flight promise and
             ;; awaits real completion rather than resolving early.
             ;;
             ;; Two-phase: (1) ask each worker's `__worker_router__`
             ;; coordinator to run its registered destroys while the Comlink
             ;; RPC channel is still live, then (2) tear down the underlying
             ;; Worker/MessagePort handles. Destroys happen in parallel across
             ;; workers; one bad destroy is logged inside the worker and
             ;; doesn't block the rest. Dead workers are skipped in phase 1
             ;; (their RPC would never settle) and live ones are capped at
             ;; SHUTDOWN-TIMEOUT-MS for the same reason. The terminated flag
             ;; is stamped on every record first so calls through in-flight
             ;; proxies reject instead of dispatching into torn-down workers.
             (fn []
               (when (nil? @terminate-promise)
                 (.forEach records-arr
                           (fn [r]
                             (set! (.-terminated r) true)
                             ;; Reject calls still in flight when terminate()
                             ;; lands; the workers are about to be torn down,
                             ;; so their Comlink calls would never settle.
                             ((.-signalReject r)
                              (js/Error. "worker-router: pool terminated"))))
                 (vreset! terminate-promise
                          (-> (js/Promise.allSettled
                               (.map records-arr
                                     (fn [r]
                                       (if (.-dead r)
                                         (js/Promise.resolve)
                                         (let [coord (aget (.-proxy r)
                                                           WORKER-ROUTER-COORDINATOR-KEY)]
                                           (if (and coord (fn? (.-shutdown coord)))
                                             (resolve-within (.shutdown coord)
                                                             shutdown-timeout)
                                             (js/Promise.resolve)))))))
                              (.then
                               (fn [_]
                                 (js/Promise.allSettled
                                  (.map records-arr
                                        (fn [r] ((.-terminate (.-handle r))))))))
                              (.then (fn [_] js/undefined)))))
               @terminate-promise)})))))

;; Class-like export mirroring the TS surface: JS callers use
;; `WorkerPool.create(opts)`, Squint callers `(.create WorkerPool opts)`.
(def WorkerPool #js {:create create-pool})
