#!/bin/bash

# compose auth profiles
KEYCLOAK_AUTH="keycloak_auth"
DEFAULT_AUTH="default_auth"

# все yaml files
all_optional_compose_yaml_files=("./docker_composes/backend_api.yaml" "./docker_composes/file_manager.yaml" "./docker_composes/general.yaml" "./docker_composes/metrics.yaml" "./docker_composes/model_manager.yaml" "./docker_composes/proxy.yaml" "./docker_composes/registry.yaml" "./docker_composes/ui.yaml")
all_required_compose_yaml_files=("./docker_composes/fs_pg.yaml" "./docker_composes/fsdb.yaml" "./docker_composes/kafka.yaml" "./docker_composes/keycloak.yaml")

proxy_url="http://proxy_default_auth:8006"
compose_profiles=()


set +e
containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

joinStr () {
  local IFS="$1"
  shift
  echo "$*"
}

main () {
  # тут будут yaml files который будем поднимать
  compose_yaml_files=()

  # пытаемся поймать --keycloak-auth, --up-all аргумент
  while [[ $# -gt 0 ]]; do
      case "$1" in
          --keycloak-auth)
              proxy_url="http://proxy_keycloak_auth:8006"
              compose_profiles+=("$KEYCLOAK_AUTH")
              shift
              ;;
          --up-all)
              compose_yaml_files=("${all_optional_compose_yaml_files[@]}")
              shift
              ;;
          *)
              echo "Invalid option: $1"
              exit 1
              ;;
      esac
  done

  # если нет опции --up-all, то в интерактивном режиме узнаем что именно надо поднять
  if [ ${#compose_yaml_files[@]} -eq 0 ]; then
    for item in "${all_optional_compose_yaml_files[@]}"; do
      read -p "Use $item compose file? (Y/N): " confirm
      if [[ $confirm == [yY] || $confirm == [yY][eE][sS] ]]; then
        compose_yaml_files+=("$item")
      fi
    done
  fi

  # если от всех сервисов отказались, то и поднимать нечего
  if [ ${#compose_yaml_files[@]} -eq 0 ]; then
    echo "Invalid state: nothing to up :("
    exit 1 ;
  fi

  # добавляем к выбранным сервисам обязательные сервисы, без которых остальное работать не будет
  # сейчас вероятно это: fs_pg, fsdb, kafka. Но кафку отсюда надо будет убрать
  for item in "${all_required_compose_yaml_files[@]}"; do
    compose_yaml_files+=("$item")
  done

  # если не поймали --keycloak-auth, то надо использовать обычный профиль авторизации
  if containsElement "$KEYCLOAK_AUTH" "${compose_profiles[@]}"; then
    echo "INFO: KeyCloak auth activated" ;
  else
    compose_profiles+=("$DEFAULT_AUTH")
  fi

  # превращаем наши профили в строку для COMPOSE_PROFILES
  compose_profiles_arg=$(joinStr , "${compose_profiles[@]}")

  # превращаем названия compose yaml файлом в строку для docker-compose c ключом -f
  compose_yaml_files_arg=""
  yaml_files_delim="-f "
  for item in "${compose_yaml_files[@]}"; do
    compose_yaml_files_arg="$compose_yaml_files_arg$yaml_files_delim$item"
    yaml_files_delim=" -f "
  done

  # в соответствии с аргументом --keycloak-auth меняем адрес для сервисов в docker-compose
  export backend_proxy_url="$proxy_url"

  COMPOSE_PROFILES=$compose_profiles_arg docker-compose $compose_yaml_files_arg up -d
}

main "$@"
