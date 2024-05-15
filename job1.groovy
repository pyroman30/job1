pipeline {
    agent any

    parameters {
        string(name: 'BRANCH', defaultValue: '', description: 'Ветка для развертывания')
    }

    stages {
        stage('Получение последней релизной ветки') {
            steps {
                script {
                    // Получение списка всех веток из репозитория
                    def gitRemoteOutput = sh(script: "git ls-remote --heads origin", returnStdout: true).trim()
                    echo "Результат выполнения команды git ls-remote --heads origin:\n${gitRemoteOutput}"

                    // Получение последней релизной ветки
                    def latestReleaseBranch = sh(script: "git ls-remote --heads origin | grep 'refs/heads/release/' | awk -F'/' '{print \$3 \"/\" \$4 \$5}' | sort -V | tail -n1", returnStdout: true).trim()
                    echo "Последняя релизная ветка: ${latestReleaseBranch}"

                    // Установка найденной ветки в параметр BRANCH
                    params.BRANCH = "$latestReleaseBranch"
                }
            }
        }

        stage('Выполнение команды в последней релизной ветке') {
            when {
                expression { params.BRANCH != '' && params.BRANCH != null }
            }
            steps {
                script {
                    // Переключение на последнюю релизную ветку
                    checkout([$class: 'GitSCM', branches: [[name: params.BRANCH]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[url: 'https://github.com/pyroman30/job1']]])

                    // Выполнение команды в последней релизной ветке
                    sh "echo 'hello'"
                }
            }
        }
    }
}
