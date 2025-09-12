# Compose files
COMPOSE_DEV = docker-compose.yml
COMPOSE_PROD = docker-compose-prod.yml

# Projects
# PROJECT_NAME_DEV is the default
PROJECT_NAME_PROD = dz_jobs_mage_prod

# ===========
# Development
# ===========

# --- Mage (dev) ---
up-dev-mage:
	docker compose -f $(COMPOSE_DEV) up mage -d

down-dev-mage:
	docker compose -f $(COMPOSE_DEV) down mage

logs-dev-mage:
	docker compose -f $(COMPOSE_DEV) logs -f mage

restart-dev-mage: down-dev-mage up-dev-mage


# --- Metabase (dev) ---
up-dev-metabase:
	docker compose -f $(COMPOSE_DEV) up metabase -d

down-dev-metabase:
	docker compose -f $(COMPOSE_DEV) down metabase

logs-dev-metabase:
	docker compose -f $(COMPOSE_DEV) logs -f metabase

restart-dev-metabase: down-dev-metabase up-dev-metabase


# ==========
# Production
# ==========

up-prod:
	./download_all_github_artifacts.sh -r
	docker compose -f $(COMPOSE_PROD) -p $(PROJECT_NAME_PROD) up mage -d

down-prod:
	docker compose -f $(COMPOSE_PROD) -p $(PROJECT_NAME_PROD) down mage

logs-prod:
	docker compose -f $(COMPOSE_PROD) -p $(PROJECT_NAME_PROD) logs -f mage

restart-prod: down-prod up-prod