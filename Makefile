export LOCAL_BUCKET_URL=data/landing_zone/

extract-remote:
	uv run dlt/northwind.py

prod-extract-remote:
	uv run dlt/northwind.py prod

extract-local:
	DESTINATION__BUCKET_URL=$(LOCAL_BUCKET_URL) $(MAKE) extract-remote

prod-extract-local:
	DESTINATION__BUCKET_URL=$(LOCAL_BUCKET_URL) $(MAKE) prod-extract-remote

plan-local:
	uv run sqlmesh -p sqlmesh plan --gateway local

prod-plan-local:
	uv run sqlmesh -p sqlmesh plan prod --gateway local

prod-elt-local:
	DESTINATION__BUCKET_URL=data/landing_zone/ $(MAKE) prod-extract-local prod-plan-local