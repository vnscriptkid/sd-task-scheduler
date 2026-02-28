up:
	docker compose up -d

down:
	docker compose down --volumes --rmi all

psql:
	docker exec -it scheduler-postgres psql -U postgres -d scheduler

v1schema:
	docker exec -i scheduler-postgres psql -U postgres -d scheduler < v1/schema.sql

v1init:
	go run v1/main.go