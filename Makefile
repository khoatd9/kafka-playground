.PHONY: up down reset status health logs dashboard open

up:            ## Start the lab (downloads the JMX agent on first run)
	@./scripts/setup.sh
	docker compose up -d
	@echo "\nKafka UI   http://localhost:8080\nGrafana    http://localhost:3000  (admin/admin)\nPrometheus http://localhost:9090"

down:          ## Stop and remove containers (Kafka data is lost)
	docker compose down

reset: down up ## Fresh cluster

status:        ## Container status
	docker compose ps

health:        ## Cluster health checklist from Prometheus
	@python3 scripts/health.py

logs:          ## Tail logs of one service: make logs s=kafka1
	docker compose logs -f --tail 100 $(s)

dashboard:     ## Regenerate the Grafana dashboard after editing scripts/gen_dashboard.py
	python3 scripts/gen_dashboard.py && docker restart grafana

open:          ## Open the three UIs in your browser (macOS)
	open http://localhost:8080 http://localhost:3000 http://localhost:9090
