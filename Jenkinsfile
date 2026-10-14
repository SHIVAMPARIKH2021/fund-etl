pipeline {
    agent any

    parameters {
        booleanParam(name: 'DRY_RUN', defaultValue: true, description: 'Run pre-flight connectivity verification without database mutations')
        string(name: 'TICKER', defaultValue: 'False', description: 'Fund ticker symbol to process')
        choice(name: 'FILING_YEAR', choices: ['2026', '2025', '2024'], description: 'SEC filing year')
        choice(name: 'FILING_QUARTER', choices: ['1', '2', '3', '4'], description: 'SEC filing quarter')
        booleanParam(name: 'FORCE_DOCKER_NO_CACHE', defaultValue: true, description: 'Force Docker to rebuild all layers without cache')
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

                    env.WINDOWS_HOST   = secrets.WINDOWS_HOST
                    env.WINDOWS_USER   = secrets.WINDOWS_USER
                    env.DATABASE_URL   = secrets.DATABASE_URL
                    env.SEC_USER_AGENT = secrets.SEC_USER_AGENT

                    echo "Successfully pulled secrets from Doppler Cloud."
                }
            }
        }

        stage('Checkout & Sync Latest Git Code') {
            steps {
                // Ensure SCM checkout updates cleanly to the tracking branch
                checkout scm

                script {
                    // Force pull and hard reset to match remote HEAD exactly
                    sh '''
                        git fetch --all --prune
                        git checkout master
                        git reset --hard origin/master
                    '''

                    // Capture latest commit SHA for unique image tagging
                    env.GIT_COMMIT_SHORT = sh(
                        script: 'git rev-parse --short HEAD',
                        returnStdout: true
                    ).trim()

                    echo "Building Docker image from commit: ${env.GIT_COMMIT_SHORT}"
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
                                    -e DATABASE_URL='${DATABASE_URL}' \
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
            echo "Pipeline run completed successfully with commit ${env.GIT_COMMIT_SHORT}."
        }
        failure {
            echo "Pipeline execution failed. Check console output above."
        }
    }
}