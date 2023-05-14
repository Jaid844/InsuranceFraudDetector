pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo "checking log"
            }
        }
        
        stage('Build') {
            steps {
                script {
                    echo 'pip install -r requirements.txt'
                    echo 'python -m pytest' // Optional: Run tests
                }
            }
        }
        
        stage('Deploy') {
            steps {
                script {
                    echo 'pip install gunicorn'
                    echo 'gunicorn -w 4 main:app' // Replace 'app' with your Flask app name
                }
            }
        }
    }
}
