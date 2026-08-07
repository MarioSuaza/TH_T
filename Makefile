# Atajos. Todo lo que hay aqui se puede ejecutar tambien con docker compose
# directamente; el Makefile solo evita teclear.

.DEFAULT_GOAL := help
.PHONY: help run dashboard test profile shell benchmark clean reset logs sql check

help:  ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

run:  ## Construye y ejecuta el pipeline completo
	docker compose up --build

dashboard:  ## Ejecuta el pipeline y deja el tablero en http://localhost:8501
	docker compose --profile dashboard up --build

test:  ## Ejecuta la suite de pruebas dentro del contenedor
	docker compose run --rm tests

profile:  ## Perfila los parquet de data/raw y escribe los reportes
	docker compose run --rm profile

shell:  ## Consola Python con la base abierta en solo lectura
	docker compose run --rm shell

benchmark:  ## Prueba de escala con datos sinteticos (usa --rows para variar el tamano)
	docker compose run --rm benchmark

sql:  ## Consulta de ejemplo sobre el estado vigente
	docker compose run --rm shell -c "import duckdb; \
	  print(duckdb.connect('/app/data/database/movements.duckdb', read_only=True) \
	        .sql('SELECT * FROM v_summary_by_fund'))"

logs:  ## Muestra el log de la ultima ejecucion
	@tail -n 60 data/reports/pipeline.log

clean:  ## Borra los contenedores y la imagen
	docker compose --profile dashboard --profile tools down --rmi local --volumes --remove-orphans

reset:  ## Borra TODAS las salidas generadas (la base incluida) y deja data/raw intacto
	rm -rf data/database
	rm -f data/reports/*.csv data/reports/*.md data/reports/*.log
	@echo "Salidas eliminadas. data/raw sigue intacto."

check: ## Pipeline + pruebas, sin Docker (requiere las dependencias instaladas)
	python -m src.pipeline --input-directory data/raw
	python -m pytest
