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


;; ───────────────────────────────────── Usage ────────────────────────────────────

(def objects ["A" "B" "C" "D" "E" "F" "G"
              "H" "I" "J" "K" "L" "M" "N"
              "O" "P" "Q" "R" "S" "T" "U"
              "V" "W" "X" "Y" "Z"])

(def objects-small (take 15 objects))


(def cache-nodes [{:name "node-0"
                   :host "localhost"
                   :port 6000}

                  {:name "node-1"
                   :host "localhost"
                   :port 6001}

                  {:name "node-2"
                   :host "localhost"
                   :port 6002}])


(defn get-mod-n-hashes
  "Returns mod `n` hashes for `objects`.
  Optionally, a custom hash function can be provided as last argument (`hash-fn`).
  This funtion should accept a single argument and return hash of that argument."
  [n objects & [hash-fn]]
  (let [hash-fn (or hash-fn generate-hash)]
    (reduce (fn [acc object]
              (let [object-hash (-> object hash-fn (mod n))]
                (update acc
                        object-hash
                        (fnil conj [])
                        object)))
            {}
            objects)))


(defn get-consistent-hashes
  [nodes objects & [hash-fn]]
  (let [hash-fn (or hash-fn hash)
        ring-state (create-ring nodes hash-fn)]
    (reduce (fn [acc object]
              (let [node (get-node ring-state object hash-fn)]
                (update acc
                        (:name node)
                        (fnil conj [])
                        object)))
            {}
            objects)))


(let [separator (->> "─" (repeat 60) cs/join)]
  (defn print-line-separator
    []
    (println separator)))


(defn mod-n-hashing-print-state
  "Prints the state of the cache for `cache-nodes` and `objects`."
  [cache-nodes objects]
  (let [nodes-count (count cache-nodes)
        mod-hashes (get-mod-n-hashes nodes-count objects)]
    (print-line-separator)
    (println (format "With %s nodes:" nodes-count))
    (pp/pprint (into (sorted-map) mod-hashes))
    (print-line-separator)
    (println "\n")))


(defn consistent-hashing-print-state
  "Prints the state of the cache for `cache-nodes` and `objects`."
  [cache-nodes objects]
  (let [nodes-count (count cache-nodes)
        mod-hashes (get-consistent-hashes cache-nodes objects)]
    (print-line-separator)
    (println (format "With %s nodes:" nodes-count))
    (pp/pprint (into (sorted-map) mod-hashes))
    (print-line-separator)
    (println "\n")))


(defn mod-n-hashing-demo
  []
  (println "Initial state")
  (mod-n-hashing-print-state cache-nodes objects-small)

  (println "Add one node")
  (mod-n-hashing-print-state (conj cache-nodes
                                   {:name "node-3"
                                    :host "localhost"
                                    :port 6003})
                             objects-small)

  (println "Initial state")
  (mod-n-hashing-print-state cache-nodes objects-small)

  (println "Remove a node")
  (mod-n-hashing-print-state (drop-last cache-nodes)
                             objects-small))


(defn consistent-hashing-demo
  []
  (println "Initial state")
  (consistent-hashing-print-state cache-nodes objects-small)

  (println "Add one node")
  (consistent-hashing-print-state (conj cache-nodes
                                        {:name "node-3"
                                         :host "localhost"
                                         :port 6003})
                                  objects-small)

  (println "Initial state")
  (consistent-hashing-print-state cache-nodes objects-small)

  (println "Remove a node")
  (consistent-hashing-print-state (drop-last cache-nodes)
                                  objects-small))


(defn -main
  "Demo different caching strategies"
  [& _]
  (println "Mod N hashing demo:")
  (print-line-separator)
  (mod-n-hashing-demo)
  (println "Consistent hashing demo:")
  (print-line-separator)
  (consistent-hashing-demo))
