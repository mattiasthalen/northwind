ingest:
	uv run dlt/northwind.py

ingest-prod:
	uv run dlt/northwind.py prod

plan-local:
	uv run sqlmesh -p sqlmesh --gateway local plan

plan-local-prod:
	uv run sqlmesh -p sqlmesh --gateway local plan prod

elt-local-prod:
	make ingest-local-prod plan-local-prod