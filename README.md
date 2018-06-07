# Asuka

a naive distributed key value database based on Go and PacificA.

Just for educational purpose, full of bugs, DO NOT use in production.

## Environment

Tested on:

Go 1.10.2

amd64

macOS High Sierra 10.13.4

Linux are not tested, but 2.6.23+ should be supported.

Do not support windows.

## install

### install Go

See https://golang.org/doc/install

### install dep

See https://github.com/golang/dep

### install Asuka
```
go get -u -v github.com/choleraehyq/asuka/cmd/...
```
Or
```
mkdir -p $GOPATH/src/github.com/choleraehyq
cd $GOPATH/src/github.com/choleraehyq
git clone https://github.com/choleraehyq/asuka.git
cd asuka
make
```

### install etcd

See https://github.com/coreos/etcd

## Run

### Run etcd

See https://github.com/coreos/etcd

### Run config manager (default config)

```
cd $GOPATH/src/github.com/choleraehyq/asuka/cmd/cm
./cm
```

### Run data node (default config)

two node:

```
cd $GOPATH/src/github.com/choleraehyq/asuka/cmd/dn
./dn
```

```
cd $GOPATH/src/github.com/choleraehyq/asuka/cmd/dn
./dn -rpc-addr=127.0.0.1:5433
```

### Run example test client

```
cd $GOPATH/src/github.com/choleraehyq/asuka/cmd/example
./example -method=create_group -group=default
./example -method=set -group=default -key=foo -value=bar
./example -method=get -group=default -key=foo

```
