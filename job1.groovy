pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                // Проверка кода из репозитория Git
                checkout scm
                sh "echo 'hello'"
            }
        }
    }
}
