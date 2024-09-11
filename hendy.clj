(ns hendy
  "Injesting Rainus data - and analysis"
  (:use [clojure.set]
        [hashp.core])
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

(def sample-layers
  "Sample layers from the drilling
  Each layer has 6 drill holes: A-F" #p
  (-> "data/samples.edn"
      slurp
      clojure.edn/read-string))

(def batch2sample
  "Mapping of batch-id from each tray in the IRMS
  to sample ids from the drilling" #p
  (-> "data/batch2sample.edn"
      slurp
      clojure.edn/read-string))

(def irms-corr
  "IRMS data - corrected 'with respect to the VPDB values'" #p
  (ds/concat (-> "data/Result_20240827_corrected.csv"
                 ds/->dataset)
             (-> "data/Result_20240822_corrected.csv"
                 ds/->dataset)))

(def samples
  "Samples with batch ids and with standards removed" #p
  (-> irms-corr
      (ds/row-mapcat (fn add-sample-name-note
                       [row]
                       (let [batch-sample-id (-> row
                                                 (get "Identifier_1")
                                                 keyword)
                             sample          (get batch2sample
                                                  batch-sample-id)]
                         [{:sample-id   (:id sample)
                           :sample-note (:note sample)}])))
      (ds/filter-column :sample-id
                        #(some? %))))
#_
(-> samples
    (ds/write! "out/table.csv"))

(defn layer-samples
  [speleo-key
   layer-key
   samples]
  (-> samples
      (ds/filter-column :sample-id
                        (fn [id]
                          (= #p (->> id
                                     symbol
                                     str
                                     (drop-last 1)
                                     (apply str))
                             (str (-> speleo-key
                                      symbol
                                      str)
                                  (-> layer-key
                                      symbol
                                      str)))))))
#_
(->> samples
     (layer-samples :LL
                    :L1))

(defn
  table2dO-dC-letter-triplet
  "Take a data table an extracts each row's data
  and put it into a 3 term vector
  [dO_corrected
   dC_corrected
   last letter of key
   tooltop with `:sample-note` is applicable]"
  [table]
  (->> table
       ds/rows
       (map (fn extract-table-row
              [row]
              (let [drill-letter (-> row
                                     :sample-id
                                     symbol
                                     str
                                     last)
                    dO           (get row
                                      "dO_corrected")
                    dC           (get row
                                      "dC_corrected")]
                [dO
                 dC
                 drill-letter
                 {:tooltip (:sample-note row)}])))))

(defn
  plot-speleo
  [speleo-key
   layers
   samples
   & [{:keys [width
              height
              x-min
              x-max
              y-min
              y-max]}]]
  (let [per-layer-points (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (layer-samples speleo-key
                                                     layer
                                                     samples)))
                              (mapv table2dO-dC-letter-triplet))
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                              (mapv table2dO-dC-letter-triplet))]
    (let [axis (quickthing/primary-axis (cond-> (apply concat per-layer-points)
                                          (and (some? x-min) ;; conditionally add dummy for min/max
                                               (some? y-min)) (conj [x-min
                                                                     y-min])
                                          (and (some? x-max)
                                               (some? y-max)) (conj [x-max
                                                                     y-max]))
                                        (cond-> {:x-name "d-Oxygen"
                                                 :y-name "d-Carbon"
                                                 :title  (str (symbol speleo-key))}
                                          (some? width)  (assoc :width width)
                                          (some? height) (assoc :height height)))]
      (cond-> (-> axis
                  (update :data
                          (fn [old-data]
                            (into old-data
                                  (flatten (mapv (fn points-to-colored-text
                                                   [layer-points
                                                    color]
                                                   (-> (mapv #(vector (first %)
                                                                      (second %))
                                                             layer-points)
                                                       (quickthing/adjustable-circles {:scale   48
                                                                                       :attribs {:fill "#8004"}})))
                                                 messy-points
                                                 colors)))))
                  (update :data
                          (fn [old-data]
                            (into old-data
                                  (flatten (mapv (fn points-to-colored-text
                                                   [layer-points
                                                    color]
                                                   (quickthing/adjustable-text layer-points
                                                                               {:scale   64
                                                                                :attribs {:fill color}}))
                                                 per-layer-points
                                                 colors)))))
                  thi.ng.geom.viz.core/svg-plot2d-cartesian)
        (some? width) (quickthing/svg-wrap [width
                                            height]
                                           width)
        (nil? width)  quickthing/svg-wrap))))
#_
(plot-speleo (-> sample-layers
                 first
                 first)
             (-> sample-layers
                 first
                 second)
             samples)
;; => {:x-axis
;;     {:scale #function[thi.ng.geom.viz.core/linear-scale/fn--39066],
;;      :major-size 10,
;;      :pos 570.0,
;;      :major (-7.0 -6.0 -5.0),
;;      :label-dist 20.571428571428573,
;;      :attribs {:stroke "black"},
;;      :label #function[thi.ng.geom.viz.core/default-svg-label/fn--39038],
;;      :label-style
;;      {:fill "black",
;;       :stroke "none",
;;       :font-family "Arial, sans-serif",
;;       :font-size 18.0,
;;       :text-anchor "start",
;;       :transform "translate(9.0 0.0)"},
;;      :minor nil,
;;      :domain [-7.36952641537735 -4.47690577325682],
;;      :minor-size 5,
;;      :visible true,
;;      :range [50.0 950.0]},
;;     :y-axis
;;     {:scale #function[thi.ng.geom.viz.core/linear-scale/fn--39066],
;;      :major-size 12.0,
;;      :pos 50.0,
;;      :major (-10.0 -9.0 -8.0 -7.0 -6.0 -5.0),
;;      :label-dist 9.0,
;;      :attribs {:stroke "black"},
;;      :label #function[thi.ng.geom.viz.core/default-svg-label/fn--39038],
;;      :label-style
;;      {:fill "black",
;;       :stroke "none",
;;       :font-family "Arial, sans-serif",
;;       :font-size 18.0,
;;       :text-anchor "end"},
;;      :minor nil,
;;      :domain [-10.528151359653489 -4.95521406789481],
;;      :minor-size 5,
;;      :label-y -9.0,
;;      :visible true,
;;      :range [570.0 30.0]},
;;     :grid
;;     {:attribs
;;      {:stroke "#caa", :stroke-dasharray "3.6 7.2", :stroke-width 0.72},
;;      :minor-x true,
;;      :minor-y true},
;;     :data
;;     [[{:values
;;        ([-5.09118384732329 -7.08277521974613 \F]
;;         [-4.47690577325682 -7.83151862412412 \A]
;;         [-5.0106014634137 -7.44407390125851 \B]
;;         [-5.87210554292892 -7.65495310123609 \C]
;;         [-5.56399072147304 -7.90480997324745 \D]
;;         [-5.56327459173114 -7.52502752779019 \E]),
;;        :shape #function[quickthing/adjustable-text/fn--39524],
;;        :layout #function[thi.ng.geom.viz.core/svg-scatter-plot]}]
;;      [{:values
;;        ([-5.20341939642241 -8.71601195104431 \A]
;;         [-6.1188122390162 -9.65630664604704 \B]
;;         [-5.75806188153009 -10.0637399186736 \C]
;;         [-4.96727561402827 -9.31117102017535 \D]
;;         [-4.90085458046631 -8.61257120603161 \E]
;;         [-5.26339526230719 -8.67087114283426 \F]),
;;        :shape #function[quickthing/adjustable-text/fn--39524],
;;        :layout #function[thi.ng.geom.viz.core/svg-scatter-plot]}]
;;      [{:values
;;        ([-6.46255281419141 -5.4196255088747 \A]
;;         [-6.46206900278289 -6.53463883482173 \B]
;;         [-6.57984252279972 -6.83404302054678 \C]
;;         [-6.81798140966473 -7.28052644144172 \D]
;;         [-6.8245474216375 -7.01674663931425 \E]
;;         [-6.74558248817552 -6.81156612614272 \F]),
;;        :shape #function[quickthing/adjustable-text/fn--39524],
;;        :layout #function[thi.ng.geom.viz.core/svg-scatter-plot]}]
;;      [{:values
;;        ([-6.82351068290496 -6.22955732421765 \A]
;;         [-7.21678024211609 -6.4707606297502 \B]
;;         [-7.36952641537735 -7.08314809988756 \C]
;;         [-7.34554979678902 -6.82638390068765 \D]
;;         [-6.86451733458201 -6.74926699548987 \E]
;;         [-6.69218814644062 -6.04203218505496 \F]),
;;        :shape #function[quickthing/adjustable-text/fn--39524],
;;        :layout #function[thi.ng.geom.viz.core/svg-scatter-plot]}]]}


(def
  all-samples-as-points
  (-> samples
      table2dO-dC-letter-triplet))

(->> sample-layers
     (mapv (fn plot-each-speleo
             [[speleo-key
               layer-list]]
             (->> (plot-speleo speleo-key
                               layer-list
                               samples
                               {:x-min (->> all-samples-as-points
                                            (mapv first)
                                            (apply min))
                                :x-max (->> all-samples-as-points
                                            (mapv first)
                                            (apply max))
                                :y-min (->> all-samples-as-points
                                            (mapv second)
                                            (apply min))
                                :y-max (->> all-samples-as-points
                                            (mapv second)
                                            (apply max))})
                  quickthing/svg2xml
                  (spit (str "out/"
                             (symbol speleo-key)
                             ".svg"))))))

(defn
  off-center-dO-plot
  [speleo-key
   layers
   samples
   & [{:keys [width
              height
              x-min
              x-max
              y-min
              y-max]}]]
  (let [per-layer-points (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (layer-samples speleo-key
                                                     layer
                                                     samples)))
                              (mapv table2dO-dC-letter-triplet)
                              (sort #(compare (-> %1
                                                  (get 2))
                                              (-> %2
                                                  (get 2))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter #p (-> values
                                                                                    (get 2))
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 first)
                                                             drill-letter])))
                                           (sort-by first)))))
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                              (mapv table2dO-dC-letter-triplet))]
    (let [axis (quickthing/primary-axis (cond-> (apply concat
                                                       per-layer-points)
                                          (and (some? x-min) ;; conditionally add dummy for min/max
                                               (some? y-min)) (conj [x-min
                                                                     y-min])
                                          (and (some? x-max)
                                               (some? y-max)) (conj [x-max
                                                                     y-max]))
                                        (cond-> {:x-name "Relative Positions"
                                                 :y-name "d-Oxygen"
                                                 :title  (str (symbol speleo-key))}
                                          (some? width)  (assoc :width width)
                                          (some? height) (assoc :height height)))]
      (->> (-> axis
               #_(update :data
                         (fn [old-data]
                           (into old-data
                                 (flatten (mapv (fn points-to-colored-text
                                                  [layer-points
                                                   color]
                                                  (-> (mapv #(vector (first %)
                                                                     (second %))
                                                            layer-points)
                                                      (quickthing/adjustable-circles {:scale   48
                                                                                      :attribs {:fill "#8004"}})))
                                                messy-points
                                                colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/dashed-line layer-points
                                                                        {:scale   32
                                                                         :attribs {:fill color}}))
                                              per-layer-points
                                              colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/adjustable-text layer-points
                                                                            {:scale   64
                                                                             :attribs {:fill color}}))
                                              per-layer-points
                                              colors)))))
               thi.ng.geom.viz.core/svg-plot2d-cartesian
               (quickthing/svg-wrap [width
                                     height]
                                    width))))))


(defn
  off-center-dC-plot
  [speleo-key
   layers
   samples
   & [{:keys [width
              height
              x-min
              x-max
              y-min
              y-max]}]]
  (let [per-layer-points (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (layer-samples speleo-key
                                                     layer
                                                     samples)))
                              (mapv table2dO-dC-letter-triplet)
                              (sort #(compare (-> %1
                                                  (get 2))
                                              (-> %2
                                                  (get 2))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter #p (-> values
                                                                                    (get 2))
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 second)
                                                             drill-letter])))
                                           (sort-by first)))))
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                              (mapv table2dO-dC-letter-triplet))]
    (let [axis (quickthing/primary-axis (cond-> (apply concat
                                                       per-layer-points)
                                          (and (some? x-min) ;; conditionally add dummy for min/max
                                               (some? y-min)) (conj [x-min
                                                                     y-min])
                                          (and (some? x-max)
                                               (some? y-max)) (conj [x-max
                                                                     y-max]))
                                        (cond-> {:x-name "Relative Positions"
                                                 :y-name "d-Carbon"
                                                 :title  (str (symbol speleo-key))}
                                          (some? width)  (assoc :width width)
                                          (some? height) (assoc :height height)))]
      (->> (-> axis
               #_(update :data
                         (fn [old-data]
                           (into old-data
                                 (flatten (mapv (fn points-to-colored-text
                                                  [layer-points
                                                   color]
                                                  (-> (mapv #(vector (first %)
                                                                     (second %))
                                                            layer-points)
                                                      (quickthing/adjustable-circles {:scale   48
                                                                                      :attribs {:fill "#8004"}})))
                                                messy-points
                                                colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/dashed-line layer-points
                                                                        {:scale   32
                                                                         :attribs {:fill color}}))
                                              per-layer-points
                                              colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/adjustable-text layer-points
                                                                            {:scale   64
                                                                             :attribs {:fill color}}))
                                              per-layer-points
                                              colors)))))
               thi.ng.geom.viz.core/svg-plot2d-cartesian
               (quickthing/svg-wrap [width
                                     height]
                                    width))))))

(->> sample-layers
     (mapv (fn plot-each-speleo
             [[speleo-key
               layer-list]]
             (->> (off-center-dO-plot speleo-key
                                      layer-list
                                      samples
                                      {:width  500
                                       :height 500
                                       :x-min  -0.5
                                       :x-max  5.5
                                       :y-min  (->> all-samples-as-points
                                                    (mapv first)
                                                    (apply min))
                                       :y-max  (->> all-samples-as-points
                                                    (mapv first)
                                                    (apply max))})
                  quickthing/svg2xml
                  (spit (str "out/"
                             (symbol speleo-key)
                             "-dO-layers"
                             ".svg")))
             (->> (off-center-dC-plot speleo-key
                                      layer-list
                                      samples
                                      {:width  500
                                       :height 500
                                       :x-min  -0.5
                                       :x-max  5.5
                                       :y-min  (->> all-samples-as-points
                                                    (mapv second)
                                                    (apply min))
                                       :y-max  (->> all-samples-as-points
                                                    (mapv second)
                                                    (apply max))})
                  quickthing/svg2xml
                  (spit (str "out/"
                             (symbol speleo-key)
                             "-dC-layers"
                             ".svg"))))))
