
(load "cl-xtdb.asd")
(load "cl-xtdb-tests.asd")

(ql:quickload "cl-xtdb-tests")

(in-package :cl-xtdb-tests)

(uiop:quit (if (run-all-tests) 0 1))
