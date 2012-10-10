APP=jdwh
APP2=jdwhput

VERSION=$(shell head -n 1 project.clj | cut -d\" -f2)
JAR=target/$(APP)-$(VERSION)-standalone.jar
EXE=target/$(APP)
EXE2=target/$(APP2)

.PHONY: help jar all test update-ref-files

help:
	@echo "Targets:"
	@echo "jar:       build the jar and executables locally."
	@echo "all:       install into system path /usr/local/bin/."
	@echo "test:      run tests against the DWH."
	@echo "update-ref-files: copy test-sql/*.out files into *.out.ref"
	@echo

jar: $(EXE) $(EXE2)

$(JAR): project.clj src/jdwh/core.clj src/jdwh/put.clj
	lein uberjar

$(EXE): $(JAR)
	cp -f bits/jdwh-jar-head.sh $@
	cat $< >>$@
	chmod +x $@

$(EXE2): $(JAR)
	cp -f bits/jdwhput.sh $@
	chmod +x $@

all: $(EXE)
	sudo install $(EXE) /usr/local/bin/$(APP)
	sudo install $(EXE2) /usr/local/bin/$(APP2)

test: $(EXE)
	bash test.sh

update-ref-files:
	for x in test-sql/*.out; do mv -f $$x $$x.ref; done
