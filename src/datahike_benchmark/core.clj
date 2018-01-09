(ns datahike-benchmark.core
  (:require [datahike.core :as d]
            [datahike.db :as db]
            [datahike.query :as q]
            [datomic.api :as dt]
            [clojure.core.async :as async]
            [criterium.core :as crit]
            [hitchhiker.konserve :as kons]
            [hitchhiker.tree.core :as hc :refer [<??]]
            [konserve.filestore :refer [new-fs-store]]))

(def db (d/db-with
         (d/empty-db {:name {:db/index true}})
         [{ :db/id 1, :name "Ivan", :age 15 }
          { :db/id 2, :name "Petr", :age 37 }
          { :db/id 3, :name "Ivan", :age 37 }
          { :db/id 4, :age 15 }]))

(def store (kons/add-hitchhiker-tree-handlers
            (async/<!! (new-fs-store "/tmp/datahike-play"))))


(def backend (kons/->KonserveBackend store))

(defn store-db [db backend]
  (let [{:keys [eavt-durable aevt-durable avet-durable]} db]
    {:eavt-key (kons/get-root-key (:tree (<?? (hc/flush-tree eavt-durable backend))))
     :aevt-key (kons/get-root-key (:tree (<?? (hc/flush-tree aevt-durable backend))))
     :avet-key (kons/get-root-key (:tree (<?? (hc/flush-tree avet-durable backend))))}))


(defn load-db [stored-db]
  (let [{:keys [eavt-key aevt-key avet-key]} stored-db
        empty                                (d/empty-db)
        eavt-durable                         (<?? (kons/create-tree-from-root-key store eavt-key))]
    (assoc empty
           :max-eid (datahike.db/init-max-eid (:eavt empty) eavt-durable)
           :eavt-durable eavt-durable
           :aevt-durable (<?? (kons/create-tree-from-root-key store aevt-key))
           :avet-durable (<?? (kons/create-tree-from-root-key store avet-key)))))

(defn load-test-data [conn db-type]
  (let [test-data (mapv (fn [n] {:db/id n
                                 :name  (str "user" n)
                                 :age   (rand-int 100)})
                        (range 10000))]
    (println "Initialize" db-type)
    (loop [n     0
           users (if (= db-type :datascript)
                   test-data
                   (mapv
                    (fn [u]
                      (assoc u :db/id (dt/tempid :db.part/user)))
                    test-data))]
      (if (empty? users)
        {:datoms n}
        (let [[txs next-txs] (split-at 100 users)]
          (recur (+ n (count (if (= db-type :datascript)
                               (d/transact! conn (vec txs))
                               @(dt/transact conn (vec txs)))))
                 next-txs))))))

(defn init-dbs []
  (let [uri             "datomic:mem://datahike"
        datomic-schema  [{:db/id                 #db/id[:db.part/db]
                          :db/ident              :name
                          :db/index              true
                          :db/valueType          :db.type/string
                          :db/cardinality        :db.cardinality/one
                          :db.install/_attribute :db.part/db}
                         {:db/id                 #db/id[:db.part/db]
                          :db/ident              :age
                          :db/valueType          :db.type/long
                          :db/cardinality        :db.cardinality/one
                          :db.install/_attribute :db.part/db}]
        datascript-conn (d/create-conn {:name {:db/index true}})
        datomic-conn (do
                       (dt/delete-database uri)
                       (dt/create-database uri)
                       (dt/connect uri))]
    @(dt/transact datomic-conn datomic-schema)
    (time (load-test-data datascript-conn :datascript))
    (time (load-test-data datomic-conn :datomic))
    (atom {:datascript (load-db (store-db @datascript-conn backend))
          :datomic    (dt/db datomic-conn)})))


(comment

  (def dbs (init-dbs))
  
  (crit/with-progress-reporting
    (crit/bench
     (dt/q '[:find ?e :where [?e :name "user99"]] (:datomic @dbs))))

  (crit/with-progress-reporting
    (crit/bench
     (d/q '[:find ?e :where [?e :name "user99"]] (:datascript @dbs))))

  (time(dt/q '[:find (count ?e) :where [?e :name _]] datomic-db))

  (time (d/q '[:find (count ?e) :where [?e :name _]] datascript-db))



  ;; ---------------------------------------------------------------
  ;; Storage
  (time (def db (d/db-with (d/empty-db {:name {:db/index true}}) test-data)))

  (time (def stored-db (store-db db backend)))

  (time (def loaded-db (load-db stored-db)))

  (d/q '[:find ?e
         :where [?e :name]] loaded-db)
  ;; => #{[3] [2] [1]}

  (def updated (d/db-with loaded-db [{:db/id -1 :name "Hiker" :age 9999}]))

  (d/q '[:find ?e :where [?e :name "Hiker"]] loaded-db)
  ;; => #{}

  (d/q '[:find ?e :where [?e :name "Hiker"]] updated)
  ;; => #{[5]}

  (time (d/q '[:find ?e :where [?e :name "user99"]] loaded-db))

  (time (d/q '[:find ?e :where [?e :age 20]] loaded-db))

  (crit/with-progress-reporting (crit/quick-bench (d/q '[:find ?e :where [?e :age 20]] loaded-db)))

  )
