(in-package :cl-xtdb)

(defparameter *headers* '(("Accept" . "application/transit+json")
                          ("Content-Type" . "application/transit+json")))
(defparameter *tx-uri* "http://localhost:3000/tx")
(defparameter *q-uri* "http://localhost:3000/query")

(defun xtdb/status ()
  (->> (drakma:http-request
        "http://localhost:3000/status"
        :method :get
        :close nil)
    (clt:decode-json)
    ))


(xtdb/status)
(defun lisp-to-edn (form)
  "Converts a Common Lisp form to an EDN string."
  (cond
    ((hash-table-p form)
     (with-output-to-string (s)
       (write-char #\{ s)
       (maphash (lambda (key value)
                  (write-string (lisp-to-edn key) s)
                  (write-char #\space s)
                  (write-string (lisp-to-edn value) s)
                  (write-char #\space s))
                form)
       (write-char #\} s)))

    ((vectorp form)
     (with-output-to-string (s)
       (write-char #\[ s)
       (loop for elem across form do
         (write-string (lisp-to-edn elem) s)
         (write-char #\space s))
       (write-char #\] s)))

    ((keywordp form)
     (concatenate 'string ":" (symbol-name form)))

    ((symbolp form)
     (symbol-name form))

    ((listp form)
     (with-output-to-string (s)
       (write-char #\( s)
       (loop for elem in form do
         (write-string (lisp-to-edn elem) s)
         (write-char #\space s))
       (write-char #\) s)))

    (t (prin1-to-string form))))

(defun xtdb/list (&rest rep)
  (make-instance
   'cl-transit:tagged-value :tag "xtdb/list"  :rep (lisp-to-edn rep)))

;; TODO use streams with drakma
(defun decode-body (body)
  (let ((input-string
          (if (typep body 'string) body
              (flexi-streams:octets-to-string body))))
    (with-input-from-string (s input-string)
      (loop while (listen s)
            collect (clt:decode (jzon:parse s :allow-multiple-content t))))))

(defun post (uri payload &optional stream)
  (if stream
      (drakma:http-request uri  :method :post
                                :accept "application/transit+json"
                                :content-type "application/transit+json"
                                :stream stream
                                :close nil
                                :content payload)
      (drakma:http-request uri  :method :post
                                :accept "application/transit+json"
                                :content-type "application/transit+json"
                                :content payload)))

;;(setf drakma:*header-stream* *standard-output*)

(defun xtdb/query (query &key tx-key stream)
  (let* ((q (if tx-key (dict :|query| query
                             :|basis| (dict :|tx| tx-key)
                             :|default-all-valid-time?| nil)
                (dict :|query| query)))
         (payload (clt:encode-json q)))
    (multiple-value-bind (body code)
        (if stream
            (drakma:http-request *q-uri* :method :post
                                         :accept "application/transit+json"
                                         :content-type "application/transit+json"
                                         :stream stream
                                         :content payload)
            (drakma:http-request *q-uri* :method :post
                                         :accept "application/transit+json"
                                         :content-type "application/transit+json"
                                         :content payload))
      (when (= code 200)
        (decode-body body)))))

(defun dump-payload (payload &optional (path #P"/tmp/payload.transit"))
  (with-open-file (file path :direction :output
                             :if-exists :append
                             :if-does-not-exist :create)
    (write-line payload file)))

(defun ->tx-key (rep)
  (declare (clt:tagged-value rep))
  (make-instance 'clt:tagged-value
                 :tag :|xtdb/xt-key|
                 :rep rep))

(defun xtdb/submit-txs (tx-ops &optional stream)
  (let* ((tx (dict :|tx-ops| tx-ops))
         (payload (clt:encode-json tx)))
    (let ((tx-key (-> (multiple-value-bind (body code)
                          (post *tx-uri* payload stream)
                        (when (= code 200) (decode-body body)))
                      (car))))
      tx-key)))

()

(defun xtdb/submit-tx (tx-op &optional stream)
  (xtdb/submit-txs (vect tx-op) stream))

(defparameter *count* 1000)

(defmethod print-object ((object hash-table) stream)
  (format stream "#HASH{~{~{(~a : ~a)~}~^ ~}}"
          (loop for key being the hash-keys of object
                  using (hash-value value)
                collect (list key value))))

(vect (vect :|put| :|crack|
            (dict :|xt/id| (uuid:make-v4-uuid)
                  :|user-id| (uuid:make-v4-uuid)
                  :|text| "yeayayaya")))

(-> (vect :|put| :|crack|!
          (dict :|xt/id| (uuid:make-v4-uuid)
                :|user-id| (uuid:make-v4-uuid)
                :|text| "yeayayaya"))
    (xtdb/submit-tx))

;; (let ((stream (nth-value 4 (drakma:http-request "http://www.lispworks.com/" :close nil))))
;;   (nth-value 2 (drakma:http-request "http://www.lispworks.com/success-stories/index.html"
;;                                     :stream stream)))


#+aaa(-> (dict
          :|find| (vect 'xt/id)
          :|where| (vect (xtdb/list
                          '$
                          :|crack| (vect 'xt/id))))
         (xtdb/query :tx-key nil)
         (length))






;; (let* ((tx (-> (vect :|put| :|crack|!
;;                      (dict :|xt/id| (incf *count*)
;;                            :|user-id| (uuid:make-v4-uuid)
;;                            :|text| "yeayayaya"))
;;                (xtdb/submit-tx :sync? nil)))

;;        (count (-> (dict
;;                    :|find| (vect 'xt/id)
;;                    :|where| (vect (xtdb/list
;;                                    '$
;;                                    :|crack| (vect 'xt/id))))
;;                   (xtdb/query :tx-key tx)
;;                   (length))))
;;   (setf *tx* tx)
;;   (values count tx)
;;   )



;; (xtdb/status)
;; (clt:encode-json (vect (uuid:make-v4-uuid)))
;; (xt/submit-tx my-node [[:put :people {:xt/id 5678
;;                                       :name "Sarah"
;;                                       :friends [{:user "Dan"}
;;                                                 {:user "Kath"}]}]])

;; ;; Datalog now supports first-class nested lookups too:
;; (xt/q my-node '{:find [friend]
;;                 :where [($ :people [friends])
;;                         [(nth friends 1) friend]]})

;; (->> (dict
;;       :|find| (vect 'x)
;;       :|where| (vect (xtdb/list
;;                       '$
;;                       :|author| (dict :|xt/*| 'x))))
;;   (xtdb/query))
