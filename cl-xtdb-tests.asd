(in-package :asdf-user)
(defsystem "cl-xtdb-tests"
  :description "Test suite for the cl-xtdb system"
  :author "Jan S. <modality@protonmail.ch>"
  :version "0.0.1"
  :depends-on (:cl-xtdb
               :fiveam)
  :license "BSD"
  :serial t
  :components ((:module "tests"
                        :serial t
                        :components ((:file "packages")
                                     (:file "test-cl-xtdb"))))

  ;; The following would not return the right exit code on error, but still 0.
  ;; :perform (test-op (op _) (symbol-call :fiveam :run-all-tests))
  )
