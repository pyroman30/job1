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
                    def latestReleaseBranch = sh(script: "git ls-remote --heads origin | grep 'refs/heads/release/' | cut -d'/' -f3 | sort -r | head -n1", returnStdout: true).trim()
                    echo '$latestReleaseBranch'

                    // Установка найденной ветки в параметр BRANCH
                    params.BRANCH = latestReleaseBranch
                }
            }
        }

        stage('Deploy') {
            when {
                expression { params.BRANCH != '' }
            }
            steps {
                echo "Deploying from branch: ${params.BRANCH}"
                // Здесь вы можете добавить команды для деплоя из найденной релизной ветки
            }
        }
    }
}
