all: 
	go test -v ./...

get-deps:
	go get -v ./...

.PHONY: get-deps
