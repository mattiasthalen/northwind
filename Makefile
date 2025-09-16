export LOCAL_BUCKET_URL=data/landing_zone/

extract-remote:
	uv run dlt/northwind.py

extract-remote-prod:
	uv run dlt/northwind.py prod

extract-local:
	DESTINATION__BUCKET_URL=$(LOCAL_BUCKET_URL) $(MAKE) extract-remote

extract-local-prod:
	DESTINATION__BUCKET_URL=$(LOCAL_BUCKET_URL) $(MAKE) extract-remote-prod

plan-local:
	uv run sqlmesh -p sqlmesh --gateway local plan

plan-local-prod:
	uv run sqlmesh -p sqlmesh --gateway local plan prod

elt-local-prod:
	DESTINATION__BUCKET_URL=data/landing_zone/ $(MAKE) extract-local-prod plan-local-prod