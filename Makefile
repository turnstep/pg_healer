MODULES = pg_healer
EXTENSION = pg_healer
DATA= pg_healer--1.0.sql
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)


test:
	prove --lib . --blib . t/


indent:
	pgindent --typedef=mytypedefs pg_healer.c
