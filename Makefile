build:
	go build -o redaktflow .

test:
	go test ./... -v

vet:
	go vet ./...

run: build
	./redaktflow
