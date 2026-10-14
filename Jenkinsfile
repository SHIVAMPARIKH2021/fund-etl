pipeline {
    agent any

    parameters {
        booleanParam(name: 'DRY_RUN', defaultValue: true, description: 'Verify database and SEC endpoint connectivity without writes')
        string(name: 'TICKER', defaultValue: 'False', description: 'Fund ticker symbol to process')
        choice(name: 'FILING_YEAR', choices: ['2026', '2025', '2024'], description: 'Target filing year')
        choice(name: 'FILING_QUARTER', choices: ['1', '2', '3', '4'], description: 'Target filing quarter')
        booleanParam(name: 'FORCE_DOCKER_NO_CACHE', defaultValue: false, description: 'Force Docker to rebuild layers from scratch')
    }

    environment {
        DOPPLER_TOKEN   = credentials('doppler-personal-token')
        DOPPLER_PROJECT = 'sec-pipeline'
        DOPPLER_CONFIG  = 'prd'
        SSH_CREDS_ID    = 'windows-laptop-ssh'
        IMAGE_BASE_NAME = 'fund-etl'
    }

    stages {
        stage('Fetch Secrets from Doppler Cloud') {
            steps {
                script {
                    def response = sh(
                        script: '''
                            curl -s --request GET \
                              --url "https://api.doppler.com/v3/configs/config/secrets/download?project=${DOPPLER_PROJECT}&config=${DOPPLER_CONFIG}&format=json" \
                              --header "accept: application/json" \
                              --header "authorization: Bearer ${DOPPLER_TOKEN}"
                        ''',
                        returnStdout: true
                    ).trim()

                    def secrets = readJSON text: response

                    // Host and connection details
                    env.WINDOWS_HOST   = secrets.WINDOWS_HOST
                    env.WINDOWS_USER   = secrets.WINDOWS_USER
                    env.SEC_USER_AGENT = secrets.SEC_USER_AGENT

                    // Discrete Database credentials
                    env.DB_USERNAME        = secrets.DB_USERNAME
                    env.DB_PASSWORD    = secrets.DB_PASSWORD
                    env.DB_HOST        = secrets.DB_HOST ?: 'host.docker.internal'
                    env.DB_PORT        = secrets.DB_PORT ?: '5432'
                    env.DB_NAME        = secrets.DB_NAME ?: 'funds'
                    env.DB_SCHEMA      = secrets.DB_SCHEMA ?: 'sec_financials'

                    echo "Discrete database secrets loaded from Doppler Cloud."
                }
            }
        }

        stage('Checkout & Sync Latest Git Code') {
            steps {
                checkout scm
                script {
                    sh '''
                        git fetch --all --prune
                        git checkout master
                        git reset --hard origin/master
                    '''
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()
                    echo "Deploying commit: ${env.GIT_COMMIT_SHORT}"
                }
            }
        }

        stage('Stream & Build Docker Image on Windows') {
            steps {
                script {
                    def noCacheFlag = params.FORCE_DOCKER_NO_CACHE ? "--no-cache" : ""
                    withCredentials([sshUserPrivateKey(credentialsId: env.SSH_CREDS_ID, keyFileVariable: 'SSH_KEY')]) {
                        sh """
                            tar -czf - . | ssh -i "\$SSH_KEY" \
                                -o StrictHostKeyChecking=no \
                                -o ConnectTimeout=15 \
                                ${WINDOWS_USER}@${WINDOWS_HOST} \
                                "docker build ${noCacheFlag} \
                                    -t ${IMAGE_BASE_NAME}:latest \
                                    -t ${IMAGE_BASE_NAME}:${GIT_COMMIT_SHORT} -"
                        """
                    }
                }
            }
        }

        stage('Execute ETL (Dry Run / Scheduled)') {
            steps {
                script {
                    def baseArgs = "--year ${params.FILING_YEAR} --quarter ${params.FILING_QUARTER} --ticker ${params.TICKER}"
                    def runArgs  = params.DRY_RUN ? "${baseArgs} --dry-run" : baseArgs

                    withCredentials([sshUserPrivateKey(credentialsId: env.SSH_CREDS_ID, keyFileVariable: 'SSH_KEY')]) {
                        sh """
                            ssh -i "\$SSH_KEY" \
                                -o StrictHostKeyChecking=no \
                                -o ConnectTimeout=15 \
                                ${WINDOWS_USER}@${WINDOWS_HOST} \
                                "docker run --rm \
                                    --add-host=host.docker.internal:host-gateway \
                                    -e DB_USERNAME='${DB_USERNAME}' \
                                    -e DB_PASSWORD='${DB_PASSWORD}' \
                                    -e DB_HOST='${DB_HOST}' \
                                    -e DB_PORT='${DB_PORT}' \
                                    -e DB_NAME='${DB_NAME}' \
                                    -e DB_SCHEMA='${DB_SCHEMA}' \
                                    -e SEC_USER_AGENT='${SEC_USER_AGENT}' \
                                    ${IMAGE_BASE_NAME}:${GIT_COMMIT_SHORT} ${runArgs}"
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
            echo "Pipeline run completed successfully for commit ${env.GIT_COMMIT_SHORT}."
        }
        failure {
            echo "Pipeline execution failed. Review logs above."
        }
    }
}