GOPATH := $(PWD):$(PWD)/vendor
export GOPATH

PATH := /usr/local/go/bin:$(PATH)
export PATH

all: build

clean:
	rm -rf bin

install-deps:
	GOPATH=$(PWD)/vendor go get github.com/influxdb/influxdb

build: clean
	go build -o bin/influxdb-proxy influxdb_proxy.go

fmt:
	go fmt influx_proxy.go
