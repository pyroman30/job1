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
                    def latestReleaseBranch = sh(script: "git ls-remote --heads origin | grep 'refs/heads/release/' | awk -F'/' '{print $3 \"/\" $4 $5}' | sort -V | tail -n1", returnStdout: true).trim()
                    echo "Последняя релизная ветка: ${latestReleaseBranch}"

                    // Установка найденной ветки в переменную окружения BRANCH
                    env.BRANCH = latestReleaseBranch
                }
            }
        }

        stage('Execute Command in Latest Release Branch') {
            when {
                expression { env.BRANCH != '' }
            }
            steps {
                echo "Выполнение команды в последней релизной ветке: ${env.BRANCH}"
                // Добавьте здесь команды для выполнения в последней релизной ветке
            }
        }
    }
}
