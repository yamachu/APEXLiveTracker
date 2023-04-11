SQLITE3:=sqlite3
DB:=db/live_events.db

sqlite/init:
	$(SQLITE3) $(DB) < migration/00_init.up.sql
