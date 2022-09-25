CURRENT_DIR=$(dirname $0)
cd $CURRENT_DIR
go build -race -buildmode=plugin -gcflags="all=-N -l" -o $CURRENT_DIR/wc.so $CURRENT_DIR/../mrapps/wc.go 
if [ -f $CURRENT_DIR/mr-out* ]; then
    rm $CURRENT_DIR/mr-out*
fi