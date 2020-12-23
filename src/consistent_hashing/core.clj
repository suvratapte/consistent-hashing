(ns consistent-hashing.core
  (:gen-class)
  (:require [clojure.pprint :as pp]))


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
  "Returns hashes for `objects`."
  [n objects]
  (reduce (fn [acc object]
            (let [object-hash (-> object
                                  generate-hash
                                  (mod n))]
              (update acc
                      object-hash
                      (fnil conj [])
                      object)))
          {}
          objects))


(def mod-hashes (get-mod-n-hashes (count cache-nodes) objects))


(defn print-line-separator
  []
  (println "────────────────────────────────────────────────────────────"))


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
