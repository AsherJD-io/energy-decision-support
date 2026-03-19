up:
	docker compose -f docker/docker-compose.yaml up -d

down:
	docker compose -f docker/docker-compose.yaml down

build:
	docker compose -f docker/docker-compose.yaml build

ingest:
	docker compose -f docker/docker-compose.yaml run --rm ingestion

logs:
	docker compose -f docker/docker-compose.yaml logs -f

deploy-flow:
	./scripts/deploy_kestra_flow.sh orchestration/kestra/energy_dss_pipeline.yml
