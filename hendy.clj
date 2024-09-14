(ns hendy
  "Injesting Rainus data - and analysis"
  (:use [clojure.set]
        [clojure.math]
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

(defn
  layer-samples
  [speleo-key
   layer-key
   samples]
  (-> samples
      (ds/filter-column :sample-id
                        (fn [id]
                          (= (->> id
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
;; => data/Result_20240827_corrected.csv [5 15]:
;;    | Row | Identifier_1 | n |    d13C/12C |   dC_STDEV |    d18O/16O |   dO_STDEV | dC_corrected | dO_corrected | dC_mean_intercept | dC_mean_slop | dO_mean_intercept | dO_mean_slop | :sample-id | :sample-note |
;;    |----:|--------------|--:|------------:|-----------:|------------:|-----------:|-------------:|-------------:|------------------:|-------------:|------------------:|-------------:|------------|--------------|
;;    |  72 |          S55 | 6 | -8.75733333 | 0.04623323 | -8.28500000 | 0.07625964 |  -9.51834081 |  -7.12861551 |       -0.66081527 |   1.01144095 |        1.91859679 |   1.09199907 |     :LLL1A |              |
;;    |  76 |          S56 | 6 | -9.22866667 | 0.02949086 | -6.09650000 | 0.05788538 |  -9.99506665 |  -4.73877555 |       -0.66081527 |   1.01144095 |        1.91859679 |   1.09199907 |     :LLL1B |              |
;;    |  77 |          S57 | 6 | -9.42516667 | 0.02605304 | -5.96233333 | 0.04564768 | -10.19381479 |  -4.59226567 |       -0.66081527 |   1.01144095 |        1.91859679 |   1.09199907 |     :LLL1C |              |
;;    |  78 |          S58 | 6 | -8.76600000 | 0.04607096 | -6.40033333 | 0.05508640 |  -9.52710664 |  -5.07056126 |       -0.66081527 |   1.01144095 |        1.91859679 |   1.09199907 |     :LLL1D |              |
;;    |  79 |          S59 | 6 | -9.25050000 | 0.03215250 | -6.26733333 | 0.04873580 | -10.01714978 |  -4.92532539 |       -0.66081527 |   1.01144095 |        1.91859679 |   1.09199907 |     :LLL1E |              |

(defn
  table2maps
  "Take a data table and convert it to a clean seq of maps"
  [table]
  (->> table
       ds/rows
       (mapv (fn extract-table-row
              [row]
              (let [sqrt-sample-num (sqrt (get row
                                               "n"))]
                {:dO (get row
                          "dO_corrected")
                 :dC (get row
                          "dC_corrected")
                 :dOerr (/ (get row
                                "dO_STDEV")
                           sqrt-sample-num)
                 :dCerr (/ (get row
                                "dC_STDEV")
                           sqrt-sample-num)
                 :letter (-> row
                             :sample-id
                             symbol
                             str
                             last)
                 :tooltip (:sample-note row)})))))

(defn
  maps2xyletter-vecs
  "Convert the seq of maps into a seq of vecs
  each vec is
  [x
   y
   letter
   tooltip]"
  [maps]
  (->> maps
       (map (fn [row]
               [(-> row
                    :dO)
                (-> row
                    :dC)
                (-> row
                    :letter)
                {:tooltip (-> row
                              :tooltip)}]))))

(defn
  maps2xyerror-vecs
  "Convert the seq of maps into a seq of vecs
  each vec is
  [x
   y
   x-err
   y-err
   tooltip]"
  [maps]
  (->> maps
       (map (fn [row]
               [(-> row
                    :dO)
                (-> row
                    :dC)
                (-> row
                    :dOerr)
                (-> row
                    :dCerr)
                {:tooltip (-> row
                              :tooltip)}]))))

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
  (let [per-layer-letters (->> layers
                               (mapv (fn layer-keys
                                       [layer]
                                       (layer-samples speleo-key
                                                      layer
                                                      samples)))
                               (mapv table2maps)
                               (mapv maps2xyletter-vecs))
        per-layer-errors  (->> layers
                               (mapv (fn layer-keys
                                       [layer]
                                       (layer-samples speleo-key
                                                      layer
                                                      samples)))
                               (mapv table2maps)
                               (mapv maps2xyerror-vecs))
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                               (mapv table2maps)
                               (mapv maps2xyletter-vecs))]
    (let [axis (quickthing/primary-axis (cond-> (apply concat per-layer-letters)
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
                                                   (quickthing/error-bars layer-points
                                                                          {:scale   64
                                                                           :attribs {:fill color}}))
                                                 per-layer-errors
                                                 colors)))))
                  (update :data
                          (fn [old-data]
                            (into old-data
                                  (flatten (mapv (fn points-to-colored-text
                                                   [layer-points
                                                    color]
                                                   (quickthing/adjustable-text layer-points
                                                                               {:scale   64
                                                                                :attribs {:fill color
                                                                                          :text-anchor "start"
                                                                                          :dominant-baseline "hanging"}}))
                                                 per-layer-letters
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


#_
(->> (-> sample-layers
                 first
                 second)
     (mapv (fn layer-keys
             [layer]
             (layer-samples (-> sample-layers
                                first
                                first)
                            layer
                            samples)))
     (mapv table2maps)
     (mapv maps2xyletter-vecs))

(def
  all-samples-as-points
  (-> samples
      table2maps
      maps2xyletter-vecs))
#_
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
                             "-dOdC-"
                             ".svg"))))))

(defn
  off-center-dO-plot
  "Just uses the drill order
  ie. A, B, C ... F
  For the X axis"
  [speleo-key
   layers
   samples
   & [{:keys [width
              height
              x-min
              x-max
              y-min
              y-max]}]]
  (let [per-layer-map    (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (layer-samples speleo-key
                                                     layer
                                                     samples)))
                              (mapv table2maps))
        per-layer-points (->> per-layer-map
                              (sort #(compare (-> %1
                                                  (get :letter))
                                              (-> %2
                                                  (get :letter))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter (-> values
                                                                                 :letter)
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 :dO)
                                                             drill-letter])))
                                           (sort-by first)))))
        per-layer-errors (->> per-layer-map
                              (sort #(compare (-> %1
                                                  (get :letter))
                                              (-> %2
                                                  (get :letter))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter (-> values
                                                                                 :letter)
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 :dO)
                                                             nil
                                                             #p (-> values
                                                                 :dOerr)])))
                                           (sort-by first)))))
        #_#_
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                              (mapv table2maps)
                              (mapv maps2xyletter-vecs))]
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
                                                (quickthing/error-bars layer-points
                                                                       #_{:scale   64
                                                                        :attribs {:fill color}}))
                                              per-layer-errors
                                              colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/adjustable-text layer-points
                                                                            {:scale   64
                                                                             :attribs {:fill color
                                                                                       :text-anchor "start"
                                                                                       :dominant-baseline "hanging"}}))
                                              per-layer-points
                                              colors)))))
               thi.ng.geom.viz.core/svg-plot2d-cartesian
               (quickthing/svg-wrap [width
                                     height]
                                    width))))))


(defn
  off-center-dC-plot
  "tbh.. it's the above function with a few tweaks"
  [speleo-key
   layers
   samples
   & [{:keys [width
              height
              x-min
              x-max
              y-min
              y-max]}]]
  (let [per-layer-map    (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (layer-samples speleo-key
                                                     layer
                                                     samples)))
                              (mapv table2maps))
        per-layer-points (->> per-layer-map
                              (sort #(compare (-> %1
                                                  (get :letter))
                                              (-> %2
                                                  (get :letter))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter (-> values
                                                                                 :letter)
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 :dC)
                                                             drill-letter])))
                                           (sort-by first)))))
        per-layer-errors (->> per-layer-map
                              (sort #(compare (-> %1
                                                  (get :letter))
                                              (-> %2
                                                  (get :letter))))
                              (mapv (fn [layer]
                                      (->> layer
                                           (map-indexed (fn make-x-pos
                                                          [pos-index
                                                           values]
                                                          (let [drill-letter (-> values
                                                                                 :letter)
                                                                letter2index {\A 0
                                                                              \B 1
                                                                              \C 2
                                                                              \D 3
                                                                              \E 4
                                                                              \F 5}]
                                                            [(get letter2index
                                                                  drill-letter)
                                                             (-> values
                                                                 :dC)
                                                             nil
                                                             #p (-> values
                                                                 :dCerr)])))
                                           (sort-by first)))))
        #_#_
        messy-points     (->> layers
                              (mapv (fn layer-keys
                                      [layer]
                                      (-> (layer-samples speleo-key
                                                         layer
                                                         samples)
                                          (ds/filter-column :sample-note
                                                            some?))))
                              (mapv table2maps)
                              (mapv maps2xyletter-vecs))]
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
                                                (quickthing/error-bars layer-points
                                                                       #_{:scale   64
                                                                        :attribs {:fill color}}))
                                              per-layer-errors
                                              colors)))))
               (update :data
                       (fn [old-data]
                         (into old-data
                               (flatten (mapv (fn points-to-colored-text
                                                [layer-points
                                                 color]
                                                (quickthing/adjustable-text layer-points
                                                                            {:scale   64
                                                                             :attribs {:fill color
                                                                                       :text-anchor "start"
                                                                                       :dominant-baseline "hanging"}}))
                                              per-layer-points
                                              colors)))))
               thi.ng.geom.viz.core/svg-plot2d-cartesian
               (quickthing/svg-wrap [width
                                     height]
                                    width))))))
#_
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

;; glue together the two smaller plot
;; then tack on the larger plot
(->> sample-layers
     (mapv (fn plot-each-speleo
             [[speleo-key
               layer-list]]
             (->> (quickthing/group-plots-grid [[(quickthing/group-plots-grid [[(off-center-dO-plot speleo-key
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
                                                                                                                  (apply max))})]
                                                                               [(off-center-dC-plot speleo-key
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
                                                                                                                  (apply max))})]])

                                                 (plot-speleo speleo-key
                                                              layer-list
                                                              samples
                                                              {:width  1000
                                                               :height 1000
                                                               :x-min  (->> all-samples-as-points
                                                                            (mapv first)
                                                                            (apply min))
                                                               :x-max  (->> all-samples-as-points
                                                                            (mapv first)
                                                                            (apply max))
                                                               :y-min  (->> all-samples-as-points
                                                                            (mapv second)
                                                                            (apply min))
                                                               :y-max  (->> all-samples-as-points
                                                                            (mapv second)
                                                                            (apply max))})]])
                  quickthing/svg2xml
                  (spit (str "out/"
                             (symbol speleo-key)
                             ""
                             ".svg"))))))
