.PHONY: help install build up down restart logs clean test lint proto

# Default target
help:
	@echo "TaskForge Platform - Available Commands"
	@echo "======================================="
	@echo "make install      - Install Python dependencies"
	@echo "make proto        - Generate protobuf stubs"
	@echo "make build        - Build Docker images"
	@echo "make up           - Start all services"
	@echo "make down         - Stop all services"
	@echo "make restart      - Restart all services"
	@echo "make logs         - View service logs"
	@echo "make test         - Run tests"
	@echo "make lint         - Run linters"
	@echo "make clean        - Clean up artifacts"
	@echo "make db-migrate   - Run database migrations"
	@echo "make demo          - Run workflow demo"

install:
	pip install -e ".[dev]"

proto:
	python -m grpc_tools.protoc \
		-I./protos \
		--python_out=./protos/gen \
		--grpc_python_out=./protos/gen \
		./protos/*.proto
	sed -i 's/import common_pb2/from . import common_pb2/' protos/gen/*_pb2_grpc.py

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f --tail=100

db-migrate:
	docker compose exec task-service python -m services.task_service.src.main migrate

demo:
	docker compose exec workflow-service python -m services.workflow_service.src.main demo

test:
	pytest tests/ -v --cov=. --cov-report=html

lint:
	black . --check
	isort . --check-only
	mypy services/ libs/

format:
	black .
	isort .

clean:
	docker compose down -v
	rm -rf dist/ build/ *.egg-info/
	rm -rf .pytest_cache/ .mypy_cache/
	rm -rf htmlcov/ .coverage
	rm -rf protos/gen/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null