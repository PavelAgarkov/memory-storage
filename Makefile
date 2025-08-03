PROTOC_IMAGE := protoc-go:31.1
DOCKERFILE   := tools/protoc/Dockerfile
API_DIR      := api
OUT_DIR      := protobuf

PROTOS := $(shell find $(API_DIR) -type f -name '*.proto')

.PHONY: proto-image proto-gen

proto-image: $(DOCKERFILE)
	@docker image inspect $(PROTOC_IMAGE) >/dev/null 2>&1 || \
	  docker build --build-arg PROTOC_VERSION=31.1 \
	                -t $(PROTOC_IMAGE) \
	                -f $(DOCKERFILE) .

proto-gen: proto-image
	docker run --rm \
	  -v "$(CURDIR)":/workspace \
	  -w /workspace \
	  $(PROTOC_IMAGE) \
	sh -c '\
	  protoc \
	    -I /googleapis \
	    -I $(API_DIR) \
	    -I . \
	    --go_out=paths=source_relative:$(OUT_DIR) \
	    --go-grpc_out=paths=source_relative:$(OUT_DIR) \
	    $(PROTOS) \
	'

run_service:
	APP_CONFIG_PATH_LOCAL=./config/dev.json APP_ENV=local go run -race ./cmd/cloud-template

generate_token:
	APP_CONFIG_PATH_LOCAL=./config/dev.json APP_ENV=local go run ./tools/token-generator $(name)


CFG = GOMEMLIMIT=256MiB APP_CONFIG_PATH_LOCAL=./config/dev.json APP_ENV=local
BIN = ./cmd/cloud-template/cloud-template-build-1
SRC = ./cmd/cloud-template

.PHONY: build_service
build_service:
	$(CFG) go build -o $(BIN) $(SRC) && $(CFG) $(BIN)

.ONESHELL:
.PHONY: gnuplot
gnuplot:
	@gnuplot <<'EOF'
	set datafile separator ","
	set term pngcairo size 1280,780
	f = "metrics.csv"

	set output "cpu.png"
	set xlabel "t, s"
	set ylabel "CPU %"
	plot f u 6:2 w l t "CPU"

	set output "rss.png"
	set ylabel "RSS (MB)"
	plot f u 6:3 w l t "RSS"

	set output "vsz.png"
	set ylabel "VSZ (MB)"
	plot f u 6:4 w l t "VSZ"

	unset output
	EOF

GO_VERSION       ?= 1.24.1
SWAG_VERSION     ?= v1.16.5

SWAGGER_IMAGE   ?= cloud-template-swag:v1.16.5
SWAGGER_MAIN    ?= cmd/cloud-template/main.go   # без ./ и с относительным путём
SWAGGER_OUT     ?= ./swagger_docs
SWAGGER_DEPTH   ?= 2

SWAGGER_DIRS := $(shell \
	go list -f '{{.Dir}}' ./cmd/... ./internal/... ./pkg/... | \
	sed 's|$(CURDIR)|.|' \
)

swagger-image:
	docker build \
	  --build-arg GO_VERSION=$(GO_VERSION) \
	  --build-arg SWAG_VERSION=$(SWAG_VERSION) \
	  -f tools/swagger/Dockerfile \
	  -t $(SWAGGER_IMAGE) .

swagger-gen: swagger-image
	@echo "Pls wait ⏩ Long operation ⏩  Catalogs for swag:" $(SWAGGER_DIRS)
	docker run --rm \
	  -v "$(CURDIR)":/workspace \
	  -w /workspace \
	  $(SWAGGER_IMAGE) \
	  init \
	    -g $(SWAGGER_MAIN) \
	    -d . \
	    -o $(SWAGGER_OUT) \
	    --parseDepth $(SWAGGER_DEPTH) \
	    --parseDependency