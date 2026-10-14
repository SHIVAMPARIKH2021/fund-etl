pipeline {
    agent any

    parameters {
        booleanParam(name: 'DRY_RUN', defaultValue: true, description: 'Run pre-flight connectivity verification without data mutations')
        choice(name: 'FILING_YEAR', choices: ['2026', '2025', '2024'], description: 'SEC filing year')
        choice(name: 'FILING_QUARTER', choices: ['1', '2', '3', '4'], description: 'SEC filing quarter')
    }

    environment {
        DOPPLER_TOKEN   = credentials('doppler-personal-token')
        DOPPLER_PROJECT = 'sec-pipeline'
        DOPPLER_CONFIG  = 'prd'
        SSH_CREDS_ID    = 'windows-laptop-ssh'
        IMAGE_NAME      = 'fund-etl:latest'
    }

    stages {
        stage('Fetch Secrets from Doppler Cloud') {
            steps {
                script {
                    def response = sh(
                        script: """
                            curl -s --request GET \
                              --url "https://api.doppler.com/v3/configs/config/secrets/download?project=${DOPPLER_PROJECT}&config=${DOPPLER_CONFIG}&format=json" \
                              --header "accept: application/json" \
                              --header "authorization: Bearer ${DOPPLER_TOKEN}"
                        """,
                        returnStdout: true
                    ).trim()

                    def secrets = readJSON text: response

                    env.WINDOWS_HOST   = secrets.WINDOWS_HOST
                    env.WINDOWS_USER   = secrets.WINDOWS_USER
                    env.DATABASE_URL   = secrets.DATABASE_URL
                    env.SEC_USER_AGENT = secrets.SEC_USER_AGENT

                    echo "Successfully pulled secrets from Doppler Cloud."
                }
            }
        }

        stage('Checkout Source Code') {
            steps {
                checkout scm
            }
        }

        stage('Stream & Build Docker Image on Windows') {
            steps {
                withCredentials([sshUserPrivateKey(credentialsId: env.SSH_CREDS_ID, keyFileVariable: 'SSH_KEY')]) {
                    sh """
                        tar -czf - . | ssh -i "\$SSH_KEY" -o StrictHostKeyChecking=no ${WINDOWS_USER}@${WINDOWS_HOST} "docker build -t ${IMAGE_NAME} -"
                    """
                }
            }
        }

        stage('Execute ETL (Dry Run / Scheduled)') {
            steps {
                script {
                    def runArgs = params.DRY_RUN ? "--dry-run" : "--year ${params.FILING_YEAR} --quarter ${params.FILING_QUARTER}"

                    withCredentials([sshUserPrivateKey(credentialsId: env.SSH_CREDS_ID, keyFileVariable: 'SSH_KEY')]) {
                        sh """
                            ssh -i "\$SSH_KEY" -o StrictHostKeyChecking=no ${WINDOWS_USER}@${WINDOWS_HOST} \
                            "docker run --rm \
                                --add-host=host.docker.internal:host-gateway \
                                -e DATABASE_URL='${DATABASE_URL}' \
                                -e SEC_USER_AGENT='${SEC_USER_AGENT}' \
                                ${IMAGE_NAME} ${runArgs}"
                        """
                    }
                }
            }
        }
    }

    post {
        always {
            cleanWs()
        }
        success {
            echo "Pipeline run completed successfully."
        }
        failure {
            echo "Pipeline execution failed. Check console output above."
        }
    }
}