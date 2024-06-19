#!/bin/bash

services=("fs_general_api" "fs_registry_api" "fs_backend_api" "fs_model_manager" "fs_metric_manager" "fs_file_manager" "fs_backend_proxy" "fs_db")

copy_requirements() {
    local service="$1"
    local source_folder="../$service"
    local destination_folder="./$service"
    local source_file="requirements.txt"

    cp "$source_folder/$source_file" "$destination_folder/main_requirements.txt"
    cp "$source_folder/$source_file" "$destination_folder/fs_requirements.txt"
}

edit_requirements() {
    local service="$1"
    local last="$2"
    local file_path=""

    if [[ "$last" == true ]]; then
        file_path="$service/fs_requirements.txt"
    else
        file_path="$service/main_requirements.txt"
    fi

    dependencies=$(cat "$file_path")

    if [[ $(echo "$dependencies" | wc -l) -ge 2 ]]; then
        if [[ "$last" == true ]]; then
            dependencies=$(tail -n 3 "$file_path")
        else
          dependencies=$(sed '$d' "$file_path" | sed '$d')
        fi
        echo "$dependencies" > "$file_path"
        echo '--extra-index-url https://binary.alfabank.ru/artifactory/api/pypi/fs_etl-pypi/simple' > temp.txt && cat "$file_path" >> temp.txt && mv temp.txt "$file_path"
    else
        echo "Файл $file_path содержит недостаточное количество зависимостей"
        exit 1
    fi
}

main() {
    for service in "${services[@]}"; do
        copy_requirements "$service"
        edit_requirements "$service" true
        edit_requirements "$service" false
    done
}

main
