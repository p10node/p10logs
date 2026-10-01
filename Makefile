CHART := charts/p10logs
REG   ?= ghcr.io/p10node
VER   ?= $(shell awk '/^version:/{print $$2}' $(CHART)/Chart.yaml)
LDFLAGS := -s -w -X github.com/p10node/p10logs/internal/agent.Version=$(VER) -X main.Version=$(VER)

.PHONY: help build test lint template template-spoke package push-chart images push-images e2e run-hub run-agent

help:
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F'##' '{printf "  %-16s %s\n",$$1,$$2}'

build: ## build agent + hub binaries into bin/
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/p10logs-agent ./cmd/p10logs-agent
	CGO_ENABLED=0 go build -trimpath -ldflags="$(LDFLAGS)" -o bin/p10logs-hub   ./cmd/p10logs-hub

test: ## unit tests
	go test ./...

e2e: build ## local end-to-end (fake /var/log/pods, real agent + hub)
	./hack/e2e-local.sh

run-hub: build ## run a hub on :8080 with ./data (UI auth: none, token: dev)
	P10_INGEST_TOKEN=dev ./bin/p10logs-hub --config hack/hub-dev.yaml

run-agent: build ## run an agent against ./hack/fakepods → localhost:8080
	P10_HUB_URL=http://localhost:8080 P10_TOKEN=dev ./bin/p10logs-agent --config hack/agent-dev.yaml

lint: ## helm lint
	helm lint $(CHART)

template: ## render standalone topology
	helm template p10logs $(CHART) --namespace p10logs

template-spoke: ## render spoke topology (agent only)
	helm template p10logs $(CHART) --namespace p10logs \
	  --set hub.enabled=false --set global.clusterName=spoke \
	  --set agent.hub.url=https://hub.example.com --set agent.hub.token=x

package: lint ## package chart -> dist/
	mkdir -p dist && helm package $(CHART) -d dist

push-chart: package ## push chart to OCI registry
	helm push dist/p10logs-$(VER).tgz oci://$(REG)/charts

images: ## build multi-arch images (docker buildx)
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VER) -f Dockerfile.agent -t $(REG)/p10logs-agent:$(VER) .
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VER) -f Dockerfile.hub   -t $(REG)/p10logs-hub:$(VER) .

push-images: ## build and push multi-arch images
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VER) -f Dockerfile.agent -t $(REG)/p10logs-agent:$(VER) --push .
	docker buildx build --platform linux/amd64,linux/arm64 --build-arg VERSION=$(VER) -f Dockerfile.hub   -t $(REG)/p10logs-hub:$(VER) --push .

e2e-kind: ## end-to-end on a kind cluster (needs docker, kind, helm, kubectl)
	./hack/e2e-kind.sh

bench: build ## local load benchmark at 2k and 10k lines/s (writes bench/RESULTS.md rows to stdout)
	@echo "| lines/s | pods | dur | ingested | agent RSS | agent CPU | hub RSS | hub CPU | raw | on disk | ratio |"
	@echo "|---|---|---|---|---|---|---|---|---|---|---|"
	@./bench/run.sh 2000 60 20
	@./bench/run.sh 10000 60 50
