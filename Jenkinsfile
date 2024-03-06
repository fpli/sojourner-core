def slack_channel = "sojourner-dev"
pipeline {
  agent {
    label 'raptor-io-builder'
  }
  tools {
    maven 'Maven3.9.6'
    jdk 'OPENJDK8'
  }
  stages {
    stage('Build') {
      steps {
        slackSend(channel: slack_channel, message: "<${BUILD_URL}|${JOB_NAME} #${BUILD_NUMBER}>: Started to build...")
        sh './scripts/ci_generate_build_num.sh'
        sh 'mvn clean test verify package'
      }
    }
    stage('Deploy') {
      steps {
        sh '[ -d "rt-pipeline" ] && ./scripts/upload_rheos_portal.sh rt-pipeline'
        sh '[ -d "dumper" ] && ./scripts/upload_rheos_portal.sh dumper'
        sh '[ -d "distributor" ] && ./scripts/upload_rheos_portal.sh distributor'
      }
    }
  }
  post {
    success {
      slackSend(channel: slack_channel, color: "good", message: "<${BUILD_URL}|${JOB_NAME} #${BUILD_NUMBER}>: :beer: Success after ${currentBuild.durationString.replace(' and counting', '')}\n job version *${readFile('pomVersion').trim()}* has been uploaded to Rheos portal")
    }
    failure {
      slackSend(channel: slack_channel, color: "danger", message: "<${BUILD_URL}|${JOB_NAME} #${BUILD_NUMBER}>: :alert: Failure after ${currentBuild.durationString.replace(' and counting', '')}")
    }
    aborted {
      slackSend(channel: slack_channel, color: "warning", message: "<${BUILD_URL}|${JOB_NAME} #${BUILD_NUMBER}>: :warning: Aborted after ${currentBuild.durationString.replace(' and counting', '')}")
    }
    unstable {
      slackSend(channel: slack_channel, color: "warning", message: "<${BUILD_URL}|${JOB_NAME} #${BUILD_NUMBER}>: :warning: Unstable after ${currentBuild.durationString.replace(' and counting', '')}")
    }
  }
}
