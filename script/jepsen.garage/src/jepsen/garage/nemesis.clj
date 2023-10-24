(ns jepsen.garage.nemesis
  (:require [clojure.tools.logging :refer :all]
            [jepsen [control :as c]
             [core :as jepsen]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.garage.daemon :as grg]
            [jepsen.control.util :as cu]))

; ---- reconfiguration nemesis ----

(defn configure-present!
  "Configure node to be active in new cluster layout"
  [test node]
  (info "configure-present!" node)
  (let [node-id (c/on node (c/exec grg/binary :node :id :-q))]
   (c/on
     (jepsen/primary test)
     (c/exec grg/binary :layout :assign (subs node-id 0 16) :-c :1G))))

(defn configure-absent!
  "Configure node to be active in new cluster layout"
  [test node]
  (info "configure-absent!" node)
  (let [node-id (c/on node (c/exec grg/binary :node :id :-q))]
   (c/on
     (jepsen/primary test)
     (c/exec grg/binary :layout :assign (subs node-id 0 16) :-g))))

(defn finalize-config!
  "Apply the proposed cluster layout"
  [test]
  (let [layout-show (c/on (jepsen/primary test) (c/exec grg/binary :layout :show))
        [_ layout-next-version] (re-find #"apply --version (\d+)\n" layout-show)]
    (info "layout show: " layout-show "; next-version: " layout-next-version)
    (c/on (jepsen/primary test)
          (c/exec grg/binary :layout :apply :--version layout-next-version))))

(defn reconfigure-subset
  "Reconfigure cluster with only a subset of nodes"
  [cnt]
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op] op
      (case (:f op)
        :start
          (let [[keep-nodes remove-nodes]
                (->> (:nodes test)
                     shuffle
                     (split-at cnt))]
            (info "layout split: keep " keep-nodes ", remove " remove-nodes)
            (run! #(configure-present! test %) keep-nodes)
            (run! #(configure-absent! test %) remove-nodes)
            (finalize-config! test)
            (assoc op :value keep-nodes))
        :stop
          (do
            (info "layout un-split: all nodes=" (:nodes test))
            (run! #(configure-present! test %) (:nodes test))
            (finalize-config! test)
            (assoc op :value (:nodes test)))))

    (teardown! [this test] this)))

; ---- nemesis scenari ----

(defn scenario-c
  "Clock scramble scenario"
  [opts]
  {:generator        (cycle [(gen/sleep 5)
                             {:type :info, :f :clock-scramble}])
   :nemesis          (nemesis/compose
                        {{:clock-scramble :scramble} (nemesis/clock-scrambler 20.0)})})

(defn scenario-cp
  "Clock scramble + partition scenario"
  [opts]
  {:generator        (->>
                       (gen/mix [{:type :info, :f :clock-scramble}
                                 {:type :info, :f :partition-stop}
                                 {:type :info, :f :partition-start}])
                       (gen/stagger 3))
   :final-generator  (gen/once {:type :info, :f :partition-stop})
   :nemesis          (nemesis/compose
                       {{:clock-scramble :scramble} (nemesis/clock-scrambler 20.0)
                        {:partition-start :start
                         :partition-stop :stop} (nemesis/partition-random-halves)})})

(defn scenario-r
  "Cluster reconfiguration scenario"
  [opts]
  {:generator        (->>
                       (gen/mix [{:type :info, :f :reconfigure-start}
                                 {:type :info, :f :reconfigure-stop}])
                       (gen/stagger 3))
   :nemesis          (nemesis/compose
                       {{:reconfigure-start :start
                         :reconfigure-stop :stop} (reconfigure-subset 3)})})

(defn scenario-pr
  "Partition + cluster reconfiguration scenario"
  [opts]
  {:generator        (->>
                       (gen/mix [{:type :info, :f :partition-start}
                                 {:type :info, :f :partition-stop}
                                 {:type :info, :f :reconfigure-start}
                                 {:type :info, :f :reconfigure-stop}])
                       (gen/stagger 3))
   :final-generator  (gen/once {:type :info, :f :partition-stop})
   :nemesis          (nemesis/compose
                       {{:partition-start :start
                         :partition-stop :stop} (nemesis/partition-random-halves)
                        {:reconfigure-start :start
                         :reconfigure-stop :stop} (reconfigure-subset 3)})})

(defn scenario-cpr
  "Clock scramble + partition + cluster reconfiguration scenario"
  [opts]
  {:generator        (->>
                       (gen/mix [{:type :info, :f :clock-scramble}
                                 {:type :info, :f :partition-start}
                                 {:type :info, :f :partition-stop}
                                 {:type :info, :f :reconfigure-start}
                                 {:type :info, :f :reconfigure-stop}])
                       (gen/stagger 3))
   :final-generator  (gen/once {:type :info, :f :partition-stop})
   :nemesis          (nemesis/compose
                       {{:clock-scramble :scramble} (nemesis/clock-scrambler 20.0)
                        {:partition-start :start
                         :partition-stop :stop} (nemesis/partition-random-halves)
                        {:reconfigure-start :start
                         :reconfigure-stop :stop} (reconfigure-subset 3)})})
