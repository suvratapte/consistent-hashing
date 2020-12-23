(ns consistent-hashing.core
  (:gen-class)
  (:require [clojure.pprint :as pp]
            [clojure.string :as cs]))


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


(defn generate-hash
  "Generates hash of `object`."
  [object]
  (hash object))


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
  (let [hash-fn (or hash-fn generate-hash)
        server-hashes (-> #(hash-fn (:name %))
                          (map nodes)
                          sort)
        hash->node (reduce (fn [acc node]
                             (assoc acc
                                    (hash (:name node))
                                    (:name node)))
                           {}
                           nodes)]
    (reduce (fn [acc object]
              (let [object-hash (hash-fn object)
                    closest-server-hash (or (-> #(< % object-hash)
                                                (take-while server-hashes)
                                                last)
                                            (last server-hashes))
                    closest-server-node (hash->node closest-server-hash)]
                (update acc
                        closest-server-node
                        (fnil conj [])
                        object)))
            {}
            objects)))


(def mod-hashes (get-mod-n-hashes (count cache-nodes) objects))


(let [separator (-> 60 (repeat "─") cs/join)]
  (defn print-line-separator
    []
    (println separator)))


(defn print-state
  "Prints the state of the cache for `cache-nodes` and `objects`."
  [cache-nodes objects]
  (let [nodes-count (count cache-nodes)
        mod-hashes (get-mod-n-hashes nodes-count
                                     objects)]
    (print-line-separator)
    (println (format "With %s nodes:" nodes-count))
    (pp/pprint (into (sorted-map) mod-hashes))
    (print-line-separator)
    (println "\n")))


(defn -main
  "Demo different caching strategies"
  [& _]
  (println "Initial state")
  (print-state cache-nodes objects-small)

  (println "Add one node")
  (print-state (conj cache-nodes
                     {:name "node-3"
                      :host "localhost"
                      :port 6003})
               objects-small)

  (println "Initial state")
  (print-state cache-nodes objects-small)

  (println "Remove a node")
  (print-state (drop-last cache-nodes)
               objects-small))
