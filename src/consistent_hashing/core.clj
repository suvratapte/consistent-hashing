(ns consistent-hashing.core
  (:gen-class)
  (:require [clojure.pprint :as pp]
            [clojure.string :as cs]))

;; ────────────────────────────────── Public API ──────────────────────────────────

(defn create-ring
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
  [ring-state node & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        current-nodes (-> ring-state :hash->node vals)
        new-nodes (-> current-nodes (conj node) set)
        node-hashes (->> new-nodes
                         (map hash-fn)
                         sort)
        hash->node (reduce (fn [acc node]
                             (assoc acc (hash node) node))
                           {}
                           new-nodes)]
    {:node-hashes node-hashes
     :hash->node hash->node}))


(defn remove-node
  [ring-state node & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        current-nodes (-> ring-state :hash->node vals set)
        new-nodes (disj current-nodes node)
        node-hashes (->> new-nodes
                         (map hash-fn)
                         sort)
        hash->node (reduce (fn [acc node]
                             (assoc acc (hash node) node))
                           {}
                           new-nodes)]
    {:node-hashes node-hashes
     :hash->node hash->node}))


(defn get-node
  [ring-state k & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        node-hashes (:node-hashes ring-state)
        key-hash (hash-fn k)
        closest-hash (or (->> node-hashes
                              (take-while #(< % key-hash))
                              last)
                         (last node-hashes))]
    (get (:hash->node ring-state) closest-hash)))

;; ────────────────────────────────────────────────────────────────────────────────
