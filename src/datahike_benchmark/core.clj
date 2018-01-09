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
        backend (kons/->KonserveBackend store)
        store (kons/add-hitchhiker-tree-handlers
               (async/<!! (new-fs-store "/tmp/datahike-play")))
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
           :datomic    (dt/db datomic-conn)
           :store store
           :backend backend})))



(defn bench-basic-query [dbs db-type]
  (let [query '[:find ?e :where [?e :name "user99"]]]
    (println "Testing simple query" db-type "...")
    (crit/with-progress-reporting
     (crit/bench
      (if (= db-type :datascript)
        (d/q query (:datascript @dbs))
        (dt/q query (:datomic @dbs)))
      :verbose))
    dbs))

(defn run-benchmarks [dbs]
  (-> dbs
      (bench-basic-query :datascript)
      (bench-basic-query :datomic)))

(defn -main [& args]
  (run-benchmarks (init-dbs)))

(comment

  (def dbs (init-dbs))

  (run-benchmarks dbs)

  (bench-basic-query dbs :datomic)

  (time (d/q '[:find (count ?e) :where [?e :name _]] (:datascript @dbs)))
  (time (dt/q '[:find (count ?e) :where [?e :name _]] (:datomic @dbs)))

  (time (d/q '[:find ?e :where [?e :age 20]] (:datascript @dbs)))
  (time (dt/q '[:find ?e :where [?e :age 20]] (:datomic @dbs)))

  )
