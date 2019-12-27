run:
	docker-compose up -d
stop:
	docker-compose down
ra:
	docker restart app
rall:
	docker-compose restart
app:
	docker exec -it app sh
la:
	docker logs app
# seeds:
# 	@echo "WITHSEEDS=withSeeds" > .env 
# 	docker-compose up -d 
# 	@echo "WITHSEEDS=''" > .env 