LISP ?= sbcl

all: test

run:
	rlwrap $(LISP) --load run.lisp

build:
	$(LISP)	--non-interactive \
		--load cl-xtdb.asd \
		--eval '(ql:quickload :cl-xtdb)' \
		--eval '(asdf:make :cl-xtdb)'

test:
	$(LISP) --non-interactive \
		--load run-tests.lisp
