(defpackage :cl-xtdb
  (:use :cl)
  (:import-from :serapeum :vect :dict :href :@)
  (:import-from :rutils :merge-hash-tables)
  (:import-from :arrow-macros
   :-> :->> :some-> :some->>
   :as-> :cond-> :cond->>
                :-<> :-<>> :some-<>
   :some-<>> :<> :<!>)
  (:local-nicknames
   (#:alex #:alexandria)
   (#:jzon #:com.inuoe.jzon))
  (:export :main))
