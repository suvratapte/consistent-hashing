(ns consistent-hashing.core
  (:gen-class)
  (:require [clojure.pprint :as pp]
            [clojure.string :as cs]))

;; ──────────────────────────────── Public API ────────────────────────────────

(defn create-ring
  "Creates a ring for `nodes` using `hash-fn` (optional).

  `nodes` is expected to be a coll of node descriptors where each descriptor
  contains name of the node under the key `:name`. This will be used for
  producing the hash for that node.

  An optional `hash-fn` can be provided. It is expected to be a function which
  takes an object and produces its hash. The hash is expected to be comparable.
  If `hash-fn` is not provided, `clojure.core/hash` will be used.

  *If `hash-fn` is provided, it is the callers responsibility to use the same
  hash function throughout the usage of this API.*


  Returns a ring data structure. It is a map which looks like this:
  ```
  {:node-hashes <a list containing hashes of `nodes` (order is preserved) >

   :hash->node <a map which is a look up table which gives node descriptor
                given a hash to a hash> }
  ```

  The return value of this function should be preserved by the caller as it is
  required as input to other functions in this API.
  "
  [nodes & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        nodes (set nodes)
        node-hashes (->> nodes
                         (map #(-> % :name hash-fn))
                         sort)
        hash->node (reduce (fn [acc node]
                             (assoc acc (-> node :name hash-fn) node))
                           {}
                           nodes)]
    {:node-hashes node-hashes
     :hash->node hash->node}))


(defn add-node
  "Adds `node` to existing `ring-state` using `hash-fn` (optional).

  `ring-state` is expected to be a ring data structure (see doc string of
  `create-ring`.)

  `node` is expected to be a node descriptor (see doc string of `create-ring`).

  An optional `hash-fn` can be provided. It is expected to be a function which
  takes an object and produces its hash. The hash is expected to be comparable.
  If `hash-fn` is not provided, `clojure.core/hash` will be used.

  *If `hash-fn` is provided, it is the callers responsibility to use the same
  hash function throughout the usage of this API.*

  Returns a ring data structure (see doc string of `create-ring`).
  "
  [ring-state node & [hash-fn]]
  (let [current-nodes (-> ring-state :hash->node vals)]
    (-> current-nodes
        (conj node)
        (create-ring (or hash-fn hash)))))


(defn remove-node
  "Removes `node` from existing `ring-state` using `hash-fn` (optional).

  `ring-state` is expected to be a ring data structure (see doc string of
  `create-ring`.)

  `node` is expected to be a node descriptor (see doc string of `create-ring`).

  An optional `hash-fn` can be provided. It is expected to be a function which
  takes an object and produces its hash. The hash is expected to be comparable.
  If `hash-fn` is not provided, `clojure.core/hash` will be used.

  *If `hash-fn` is provided, it is the callers responsibility to use the same
  hash function throughout the usage of this API.*

  Returns a ring data structure (see doc string of `create-ring`).
  "
  [ring-state node & [hash-fn]]
  (let [current-nodes (-> ring-state :hash->node vals)]
    (-> current-nodes
        (remove #(= node %))
        (create-ring (or hash-fn hash)))))


(defn get-node
  "Returns node descriptor for object `k` using `hash-fn` (optional).

  An optional `hash-fn` can be provided. It is expected to be a function which
  takes an object and produces its hash. The hash is expected to be comparable.
  If `hash-fn` is not provided, `clojure.core/hash` will be used.

  *If `hash-fn` is provided, it is the callers responsibility to use the same
  hash function throughout the usage of this API.*
  "
  [ring-state k & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        node-hashes (:node-hashes ring-state)
        key-hash (hash-fn k)
        closest-hash (or (->> node-hashes
                              (take-while #(< % key-hash))
                              last)
                         (last node-hashes))]
    (get (:hash->node ring-state) closest-hash)))

;; ────────────────────────────────────────────────────────────────────────────
