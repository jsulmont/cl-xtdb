(in-package :asdf-user)

(defsystem "cl-xtdb"
  :author "Jan Sulmont <modality@protonmail.ch>"
  :version "0.0.1"
  :license "MIT"
  :description "CL access to XTDB2"
  :homepage ""
  :bug-tracker ""
  :source-control (:git "")

  ;; Dependencies.
  :depends-on (:cl-transit
               :alexandria
               :dexador
               :flexi-streams
               :serapeum
               :rutils
               :arrow-macros
               :bordeaux-threads
               :com.inuoe.jzon
               :uuid
               :drakma)

  ;; Project stucture.
  :serial t
  :components ((:module "src"
                :serial t
                :components ((:file "packages")
                             (:file "cl-xtdb"))))

  ;; Build a binary:
  ;; don't change this line.q
  :build-operation "program-op"
  ;; binary name: adapt.
  :build-pathname "cl-xtdb"
  ;; entry point: here "main" is an exported symbol. Otherwise, use a double ::
  :entry-point "cl-xtdb:main")
