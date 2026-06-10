.PHONY: build run chart test fmt vet clean

build:
	go build -o nimbus ./cmd/nimbus

run: build
	./nimbus

chart: build
	./nimbus -svg results.svg -json results.json

test:
	go vet ./...
	go test ./...

fmt:
	gofmt -w .

clean:
	rm -f nimbus results.svg results.json
