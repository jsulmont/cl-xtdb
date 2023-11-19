(in-package :cl-xtdb)

(defparameter *headers* '(("Accept" . "application/transit+json")
                          ("Content-Type" . "application/transit+json")))

(defmethod print-object ((object hash-table) stream)
  (format stream "#HASH{~{~{(~a : ~a)~}~^ ~}}"
          (loop for key being the hash-keys of object
                  using (hash-value value)
                collect (list key value))))

;; "A class representing a client for the XTDB database.
;;  It stores the base URL and the derived URIs for transactions, queries,
;;  and status operations. The class also keeps track of the latest
;;  submitted transaction and the thread that owns the client instance."
(defclass xtdb-http-client ()
  ((url :initarg :url :reader url)
   (tx-uri :accessor tx-uri :initform nil)
   (query-uri :accessor query-uri :initform nil)
   (status-uri :accessor status-uri :initform nil)
   (latest-submitted-tx :accessor latest-submitted-tx :initform nil)
   (owner :accessor owner :initform (bt:current-thread))))


(defmethod initialize-instance :after ((client xtdb-http-client) &key)
  (with-slots (url tx-uri query-uri status-uri) client
    (setf tx-uri (format nil "~a/tx" url)
          query-uri (format nil "~a/query" url)
          status-uri (format nil "~a/status" url))))

;;=> body-or-stream0, status-code1, headers2, uri3, stream4, must-close5, reason-phrase6
(defun make-xtdb-http-client (uri)
  (make-instance 'xtdb-http-client :url uri))

(define-condition not-owner-error (error)
  ((text :initarg :text :reader text)))

(define-condition xtdb-error (error)
  ((code :initarg :code :reader code)
   (reason :initarg :reason :reader reason)))

(defmethod print-object ((e xtdb-error) out)
  "Initializes an instance of xtdb-http-client,
   setting up URIs for transaction, query,
   and status based on the base URL."
  (with-slots (code reason) e
    (format out "#<XTDB-ERROR code: ~a reason: ~a>"
            code reason)))

(defmethod status ((client xtdb-http-client))
  (with-slots (status-uri) client
    (multiple-value-bind (body status)
        (drakma:http-request status-uri)
      (if (= 200 status)
          (clt:decode-json body)
          (error 'http-failed :status status)))))

(defparameter *http-client-format*
  "#<~a~% tx-uri: ~a~% query-uri: ~a~% status-uri: ~a~% latest-submitted-tx: ~a~%>")

(defmethod print-object ((client xtdb-http-client) out)
  (with-slots (tx-uri query-uri status-uri
               latest-submitted-tx http-stream)
      client
    (format out *http-client-format*
            (type-of client) tx-uri query-uri
            status-uri latest-submitted-tx)))

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

(defun post (tx-uri content)
  (multiple-value-bind (body status headers uri stream close? reason)
      (drakma:http-request tx-uri
                           :method :post
                           :accept "application/transit+json"
                           :content-type "application/transit+json"
                           :content content)
    (declare (ignore headers uri stream close?))
    (values body status reason)))

(defmethod submit-tx ((client xtdb-http-client) tx-ops)
  (check-type tx-ops (and (not string) vector))
  (ensure-local client)
  (let* ((tx (dict :|tx-ops| tx-ops))
         (content (clt:encode-json tx)))
    (with-slots (tx-uri) client
      (multiple-value-bind (body code reason)
          (post tx-uri content)
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

(defmethod query ((client xtdb-http-client) query-map &key tx-key)
  (check-type query-map hash-table)
  (let* ((q (if tx-key (dict :|query| query-map
                             :|basis| (dict :|tx| tx-key)
                             :|default-all-valid-time?| nil)
                (dict :|query| query-map)))
         (content (clt:encode-json q)))
    (multiple-value-bind (body code reason)
        (post (slot-value client 'query-uri) content)
      (if (/= code 200)
          (error 'xtdb-error :code code :reason reason)
          (decode-body body)))))

(defun test ()
  (let*  ((node (make-xtdb-http-client "http://localhost:3000"))
          (xt-id (uuid:make-v4-uuid))
          (tx-key (submit-tx node
                             (vect (vect :|put| :|clock|
                                         (dict :|xt/id| xt-id
                                               :|user-id| (uuid:make-v4-uuid)
                                               :|text| "yeayayaya"))))))
    (query node
           (dict
            :|find| (vect 'x)
            :|where| (vect (xtdb/list
                            '$
                            :|clock| (dict :|xt/*| 'x))))
           :tx-key tx-key)


    ))

(defun main ()
  (let ((node (make-xtdb-http-client "http://localhost:3000"))
        (count 0))
    (loop
      (submit-tx node
                 (vect (vect :|put| :|clock|
                             (dict :|xt/id| (uuid:make-v4-uuid)
                                   :|user-id| (uuid:make-v4-uuid)
                                   :|text| "yeayayaya"))))

      (sleep 0.01)
      (when (= 0 (mod (incf count) 10))
        (format t "--> ~a ~%" count)))))

;; (defun main ()
;;   "Entry point for the executable.
;;    Reads command line arguments."
;;   ;; uiop:command-line-arguments returns a list of arguments (sans the script name).
;;   ;; We defer the work of parsing to %main because we call it also from the Roswell script.
;;   (format t "~a~%" (test))
;;   (uiop:quit)
;;   )
