(ns datahike-benchmark.core
  (:require [datahike.core :as d]
            [datahike.db :as db]
            [datahike.query :as q]
            [datascript.core :as ds]
            [datascript.db :as dsb]
            [datascript.query :as qs]
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


(defn load-db [store stored-db]
  (let [{:keys [eavt-key aevt-key avet-key]} stored-db
        empty                                (d/empty-db)
        eavt-durable                         (<?? (kons/create-tree-from-root-key store eavt-key))]
    (assoc empty
           :max-eid (datahike.db/init-max-eid (:eavt empty) eavt-durable)
           :eavt-durable eavt-durable
           :aevt-durable (<?? (kons/create-tree-from-root-key store aevt-key))
           :avet-durable (<?? (kons/create-tree-from-root-key store avet-key)))))

(defn load-test-data [conn db-type sample-size]
  (let [test-data (mapv (fn [n] {:db/id n
                                 :name  (str "user" n)
                                 :age   (rand-int 100)})
                        (range sample-size))]
    (println "Initialize" db-type)
    (loop [n     0
           users (if (or (= db-type :datahike) (= db-type :datascript))
                   test-data
                   (mapv
                    (fn [u]
                      (assoc u :db/id (dt/tempid :db.part/user)))
                    test-data))]
      (if (empty? users)
        {:datoms n}
        (let [[txs next-txs] (split-at 100 users)]
          (recur (+ n (count (case db-type
                               :datahike (d/transact! conn (vec txs))
                               :datascript (ds/transact! conn (vec txs))
                               :datomic @(dt/transact conn (vec txs)))))
                 next-txs))))))

(defn init-dbs [sample-size]
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
        datahike-conn (d/create-conn {:name {:db/index true}})
        datascript-conn (ds/create-conn {:name {:db/index true}})
        datomic-conn (do
                       (dt/delete-database uri)
                       (dt/create-database uri)
                       (dt/connect uri))]
    @(dt/transact datomic-conn datomic-schema)
    (time (load-test-data datahike-conn :datahike sample-size))
    (time (load-test-data datomic-conn :datomic sample-size))
    (time (load-test-data datascript-conn :datascript sample-size))
    (atom {:datahike datahike-conn
           :datascript datascript-conn
           :datomic    (dt/db datomic-conn)})))

(defn benchmark-query-by-type [dbs db-type query]
  (println "Testing query" query "on" db-type "...")
  (crit/with-progress-reporting
    (crit/bench
     (case db-type
       :datahike   (d/q query (-> dbs deref :datahike deref))
       :datascript (ds/q query (-> dbs deref :datascript deref))
       :datomic    (dt/q query (:datomic @dbs)))
     :verbose))
  dbs)

(defn benchmark-query [dbs query]
  (-> dbs
      (benchmark-query-by-type :datahike query)
      (benchmark-query-by-type :datomic query)
      (benchmark-query-by-type :datascript query)))

(defn run-benchmarks [dbs]
  (-> dbs
      (benchmark-query '[:find ?e :where [?e :name "user99"]])
      (benchmark-query '[:find ?e :where [?e :age ?a] [(< 20 ?a)] [(< ?a 30)]])
      (benchmark-query '[:find (count ?e) :where [?e :name _]])
      (benchmark-query '[:find (count ?e) :where [?e :age ?a] [(< 20 ?a)] [(< ?a 30)]])
      )
  true)

(defn -main [& args]
  (run-benchmarks (init-dbs 100000)))

(comment

  (def dbs (init-dbs 100000))

  (run-benchmarks dbs)

  )
