#!/bin/bash
cd ../fs_db

#Заполнение схемы stable_users;
stand=localhost_docker_users fs-db users regenerate --use-alembic

#Заполнение  схемы stable_registry
stand=localhost_docker_registry fs-db registry regenerate --use-alembic

#Заполнение  схемы stable_general
stand=localhost_docker_general fs-db general regenerate --use-alembic

#Заполнение  схемы stable_general mm_stable
stand=localhost_docker_metrics fs-db metrics regenerate --use-alembic

#Заполнение схемы stable_model_manager
stand=localhost_docker_model fs-db models regenerate --use-alembic