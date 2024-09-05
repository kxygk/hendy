(ns hendy
  "Injesting Rainus data - and analysis"
  (:use [clojure.set])
  (:require [quickthing]
            [convertadoc]
            [tick.core       :as tick]
            [tech.v3.dataset :as ds ]
            [clojure.java.io :as io]
            [dk.ative.docjure.spreadsheet :as docjure]
            [clojure.edn     :as edn ]))

(def colors
  (-> "data/colors.edn"
      slurp
      clojure.edn/read-string))

(def samples
  (-> "data/samples.edn"
      slurp
      clojure.edn/read-string))

(def batch2sample (-> "data/batch2sample.edn"
                      slurp
                      clojure.edn/read-string))

(def irms-corr
  "IRMS data - corrected 'with respect to the VPDB values'"
  (ds/concat (-> "data/Result_20240827_corrected.csv"
                 ds/->dataset)
             (-> "data/Result_20240822_corrected.csv"
                 ds/->dataset)))

(def samples
  "Samples with batch ids and with standards removed"
  (-> irms-corr
      (ds/row-mapcat (fn add-sample-name-note
                       [row]
                       (let [batch-sample-id (-> row
                                                 (get "Identifier_1")
                                                 keyword)
                             sample          (get batch2sample
                                         batch-sample-id)]
                         [{:sample-id  (:id sample)
                           :sample-note (:note sample)}])))
      (ds/filter-column :sample-id
                        #(some? %))))
                       
                       
                       
