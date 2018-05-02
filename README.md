# Asuka

a naive distributed key value based on Go and PacificA.

Just for educational purpose, full of bugs, DO NOT use in production.

## install

### install Go

See https://golang.org/doc/install

### install Asuka

go get -u -v github.com/choleraehyq/asuka/cmd/...

### install etcd

See https://github.com/coreos/etcd

## Run

### Run etcd

See https://github.com/coreos/etcd

### Run config manager (default config)

```
cm
```

### Run data node (default config)

two node:

```
dn
```

```
dn -rpc-addr=127.0.0.1:5433
```

### Run example test client

```
./example -method=create_group -group=default
./example -method=set -group=default -key=foo -value=bar
./example -method=get -group=default -key=foo

```
