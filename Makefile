.PHONY: build run chart test integration fmt vet clean

# Build both binaries: the orchestrator daemon and the simulation CLI.
build:
	go build -o nimbusd ./cmd/nimbusd
	go build -o nimbus ./cmd/nimbus

# Simulation benchmark (the "brain").
run:
	go build -o nimbus ./cmd/nimbus
	./nimbus

chart:
	go build -o nimbus ./cmd/nimbus
	./nimbus -svg results.svg -json results.json

# Hermetic unit tests — no Docker required.
test:
	go vet ./...
	go test ./...

# End-to-end tests against a real Docker daemon (build tag `integration`).
integration:
	go test -tags=integration -v ./test/integration/...

fmt:
	gofmt -w .

vet:
	go vet ./...

clean:
	rm -f nimbus nimbusd results.svg results.json
