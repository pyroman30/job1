pipeline {
    agent any

    stages {
        stage('Hello') {
            steps {
                sh "echo 'hello hello'"
            }
        }
    }
}