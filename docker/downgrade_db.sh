#!/bin/bash
cd ../fs_db

#Очистка схемы stable_users
stand=localhost_docker_users fs-db users drop 

#Очистка схемы stable_registry
stand=localhost_docker_registry fs-db registry drop 

#Очистка схемы stable_general
stand=localhost_docker_general fs-db general drop 

#Очистка схемы stable_general mm_stable
stand=localhost_docker_metrics fs-db metrics drop 

#Очистка схемы stable_model_manager
stand=localhost_docker_model fs-db models drop