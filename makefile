
up:
	docker compose --env-file env up --build -d

down:
	docker compose --env-file env down 

sh:
	docker exec -ti loader bash

run-etl:
	docker exec loader python load_data.py

warehouse:
	docker exec -ti warehouse psql postgres://sahi:sdepassword1234@localhost:5432/warehouse

pytest:
	docker exec loader pytest -p no:warnings -v /opt/sahi/test

format:
	docker exec loader python -m black -S --line-length 120 /opt/sahi/
	docker exec loader isort /opt/sahi/

type:
	docker exec loader mypy --ignore-missing-imports /opt/sahi/

lint:
	docker exec loader flake8 /opt/sahi

ci: format type lint pytest
