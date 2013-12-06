all:
	go build

deps:
	go get menteslibres.net/gosexy/redis

test_run:
	./blpop-proxy 127.0.0.1:6379 127.0.0.1:6378 hi test ping queue bang
