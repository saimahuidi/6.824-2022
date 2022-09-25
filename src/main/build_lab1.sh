RACE="-race"
echo $RACE
cd "$(dirname $0)"
# run the test in a fresh sub-directory.
rm -f mr-*
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin -gcflags="all=-N -l" nocrash.go) || exit 1
# (cd .. && go build $RACE mrcoordinator.go) || exit 1
# (cd .. && go build $RACE mrworker.go) || exit 1
# (cd .. && go build $RACE mrsequential.go) || exit 1
