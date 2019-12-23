run:
	docker-compose up -d
stop:
	docker-compose down
ra:
	docker restart app
rd:
	docker restart mongo
rall:
	docker-compose restart
mongo:
	docker exec -it mongo bash
app:
	docker exec -it mongo sh
la:
	docker logs app
lm:
	docker logs mongo
seeds:
	@echo "WITHSEEDS=withSeeds" > .env 
	docker-compose up -d 
	@echo "WITHSEEDS=''" > .env 