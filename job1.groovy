pipeline {
    agent any

    parameters {
        string(name: 'BRANCH', defaultValue: '', description: 'Branch to deploy from')
    }

    stages {
        stage('Get Latest Release Branch') {
            steps {
                script {
                    // Получение последней релизной ветки
                    def latestReleaseBranch = sh(script: "git ls-remote --heads origin | grep 'refs/heads/release/' | awk -F'/' '{print \$3 \"/\" \$4 \$5}' | sort -V | tail -n1", returnStdout: true).trim()
                    echo "$latestReleaseBranch"

                    // Установка найденной ветки в параметр BRANCH
                    params.BRANCH = latestReleaseBranch
                }
            }
        }

        stage('Execute Command in Latest Release Branch') {
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
