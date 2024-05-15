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
                    def releaseBranches = sh(script: "git ls-remote --heads origin | grep 'refs/heads/release/'", returnStdout: true).trim().split('\n')
                    def latestReleaseBranch = ''
                    def latestVersion = 0
                    
                    releaseBranches.each { branch ->
                        def version = branch.tokenize('refs/heads/release/job1-')[1].tokenize('.')[0].toInteger()  // Извлечение версии из названия ветки
                        if (version > latestVersion) {
                            latestVersion = version
                            latestReleaseBranch = branch.tokenize('refs/heads/')[1]
                        }
                    }
                    
                    echo "Latest release branch: $latestReleaseBranch"
                    
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
