(in-package :cl-xtdb)

(declaim (optimize (speed 3) (debug 0) (safety 0)))

;; "A class representing a client for  XTDB2. It stores the base and
;;  derived URLs (transactions, queries, and status). The class also
;;  keeps track of the latest submitted tx (by this client) and the
;;  thread that owns this instance."
(defclass xtdb-http-client ()
  ((url :initarg :url :reader url)
   (tx-url :accessor tx-url :initform nil)
   (query-url :accessor query-url :initform nil)
   (status-url :accessor status-url :initform nil)
   (latest-submitted-tx :accessor latest-submitted-tx :initform nil)
   (owner :accessor owner :initform (bt:current-thread))
   (http-stream :accessor http-stream :initform nil)))

(defmethod initialize-instance :after ((client xtdb-http-client) &key)
  (with-slots (url tx-url query-url status-url http-stream) client
    (setf tx-url (format nil "~a/tx" url)
          query-url (format nil "~a/query" url)
          status-url (format nil "~a/status" url)
          http-stream (nth-value 4 (drakma:http-request status-url :close nil)))))

(defmethod print-object ((object hash-table) stream)
  (format stream "#HASH{~{~{(~a : ~a)~}~^ ~}}"
          (loop for key being the hash-keys of object
                  using (hash-value value)
                collect (list key value))))

(defparameter *http-client-format*
  "#<~a url: ~a stream: ~a latest-submitted-tx: ~a>")

(defun stream-status (stream)
  (let ((status nil))
    (if (open-stream-p stream)
        (progn
          (when (output-stream-p stream)
            (push :OUTPUT status))
          (when (input-stream-p stream)
            (push :INPUT status)))
        (push :CLOSED status))))

(defmethod print-object ((client xtdb-http-client) out)
  (with-slots (url latest-submitted-tx http-stream)
      client
    (format out *http-client-format*
            (type-of client) url (stream-status http-stream)
            latest-submitted-tx)))

(defun make-xtdb-http-client (url)
  (make-instance 'xtdb-http-client :url url))

(define-condition not-owner-error (error)
  ((text :initarg :text :reader text)))

(define-condition xtdb-error (error)
  ((code :initarg :code :reader code)
   (reason :initarg :reason :reader reason)))

(defmethod print-object ((e xtdb-error) out)
  "Initializes an instance of xtdb-http-client,
   setting up URLs for transaction, query,
   and status based on the base URL."
  (with-slots (code reason) e
    (format out "#<XTDB-ERROR code: ~a reason: ~a>"
            code reason)))

(defmethod status ((client xtdb-http-client))
  (with-slots (status-url) client
    (multiple-value-bind (body status)
        (drakma:http-request status-url)
      (if (= 200 status)
          (clt:decode-json body)
          (error 'http-failed :status status)))))

;; TODO Slime etc
(defvar *whitelisted-threads* (list "slynk-worker")
  "A list of threads that are allowed to access certain functions.")

(defun thread-whitelisted-p ()
  "Check if the current thread is in the whitelist."
  (member (bt:thread-name (bt:current-thread))
          *whitelisted-threads* :test #'string=))

(defmethod ensure-local ((client xtdb-http-client))
  (with-slots (owner) client
    (unless (or (thread-whitelisted-p)
                (eq (bt:current-thread) owner))
      (error 'not-owner-error
             :text (format nil "owner: ~a calling: ~a"
                           owner (bt:current-thread))))))

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

    ((typep form 'uuid:uuid)
     (format nil "#uuid \"~a\"" form))

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

;; TODO
;; Probably ask Drakma to return a stream
;; meaning we have to get clt:decode-json
;; to accept streams ??
(defun decode-body (body)
  (let ((input-string
          (if (typep body 'string) body
              (flexi-streams:octets-to-string body))))
    (with-input-from-string (s input-string)
      (loop while (listen s)
            collect (clt:decode
                     (jzon:parse s :allow-multiple-content t))))))

(defun dump-payload (payload &optional (path #P"/tmp/payload.transit"))
  (with-open-file (file path :direction :output
                             :if-exists :append
                             :if-does-not-exist :create)
    (write-line (prin1-to-string payload) file)))

(defun post (tx-url content http-stream)
  (multiple-value-bind (body status headers url stream close? reason)
      (drakma:http-request tx-url
                           :method :post
                           :stream http-stream
                           :accept "application/transit+json"
                           :content-type "application/transit+json"
                           :close nil
                           :content content)
    (declare (ignore headers url stream close?))
    (values body status reason)))

(defmethod submit-tx ((client xtdb-http-client) tx-ops)
  (check-type tx-ops (and (not string) vector))
  (ensure-local client)
  (let* ((tx (dict :|tx-ops| tx-ops))
         (content (clt:encode-json tx)))
    (with-slots (tx-url http-stream) client
      (multiple-value-bind (body code reason)
          (post tx-url content http-stream)
        (if (/= code 200)
            (error 'xtdb-error :code  code :reason reason)
            (let ((tx-key (car (decode-body body))))
              (setf (latest-submitted-tx client) tx-key)))))))

(defun ->tx-key (rep)
  (declare (clt:tagged-value rep))
  (make-instance 'clt:tagged-value
                 :tag :|xtdb/xt-key|
                 :rep rep))

(defun xtdb/list (&rest rep)
  (make-instance
   'cl-transit:tagged-value :tag "xtdb/list"  :rep (lisp-to-edn rep)))

(defun build-query (query-map basis basis-timeout args default-all-valid-time? default-tz)
  (let ((result (dict :|query| query-map)))
    (when basis
      (setf result (merge-hash-tables
                    result (dict :|basis| basis))))
    (when basis-timeout
      (setf result (merge-hash-tables
                    result (dict :|basis-timeout| basis-timeout))))
    (when args
      (setf result (merge-hash-tables
                    result (dict :|args| args))))
    (when default-all-valid-time?
      (setf result (merge-hash-tables
                    result (dict :|default-all-valid-time?|
                                 default-all-valid-time?))))
    (when default-all-valid-time?
      (setf result (merge-hash-tables
                    result (dict :|default-tz| default-tz))))
    result))

(defmethod query ((client xtdb-http-client) query-map
                  &key basis basis-timeout args default-all-valid-time? default-tz )
  (check-type query-map hash-table) ;; TODO check keyed args
  (let* ((q (build-query query-map basis basis-timeout args default-all-valid-time? default-tz))
         (content (clt:encode-json q)))
    (with-slots (query-url http-stream) client
      (multiple-value-bind (body code reason)
          (post query-url content http-stream)
        (if (/= code 200)
            (error 'xtdb-error :code code :reason reason)
            (decode-body body))))))

(> 1 2)

(defmacro my-time (&body body)
  `(let ((start-time (get-internal-real-time)))
     (multiple-value-bind (result &rest ignored)
         (multiple-value-call #'values
           (progn ,@body))
       (values (/ (- (get-internal-real-time) start-time) 1000.0) result))))

(defun read-args (argv)
  (let ((url "http://localhost:3000")
        (table :foobar))
    (when (plusp (length argv))
      (setf table (intern (string-left-trim ":" (first argv)) :keyword)))
    (when (> (length argv) 1)
      (setf url (cadr argv)))
    (values table url)))

(defun %main (argv)
  (multiple-value-bind (table url)
      (read-args argv)
    (let ((node (make-xtdb-http-client "http://localhost:3000"))      )
      (format t "-->url: ~a  table: ~a ~%" url table)
      (loop
        for count from 1 upto 500000
        do (let* ((xt/id (uuid:make-v4-uuid))
                  (tx-key (submit-tx
                           node
                           (vect (vect :|put| table
                                       (dict :|xt/id| xt/id
                                             :|user-id| (uuid:make-v4-uuid)
                                             :|text| "yeayayaya")))))
                  (rc (query node
                             (dict
                              :|find| (vect 'x)
                              :|where| (vect (xtdb/list
                                              '$
                                              table (dict :|xt/*| 'x
                                                          :|xt/id| xt/id))))
                             :basis (dict :|tx| tx-key)
                             :default-all-valid-time? nil)))
             ;;(format t "xt/id: ~a res: ~a ~%" xt/id (href (car rc) :|x| :|xt/id|))
             (assert (and (= 1 (length rc))
                          (uuid:uuid= xt/id (href (car rc) :|x| :|xt/id|))))
             (sleep 0.005)
             (when (= 0 (mod count 10))
               (format t "--> count=~a~%" count)))))))

(defun main ()
  (%main (uiop:command-line-arguments)))
