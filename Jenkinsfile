pipeline {
    agent {
        node {
            label 'docker'
        }
    }

    stages {
        stage ('checkout') {
            steps {
                checkout scm
            }
        }

        stage ('build') {
            steps {
                // Report status of stage 'build' back to Gitlab
                gitlabCommitStatus(name: 'build') {
                    // Populate changelog file with version information from git tag
                    dir ('debian') {
                        sh './make-changelog'
                    }

                    script {
                        //---------------------------
                        // Build Ubuntu 18.04 package
                        //---------------------------
                        def buildDir1804 = 'pkg-build/1804'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir1804}"

                        sh './package 1804'

                        publishPackages buildDir1804.toString(), 'common/bionic/stable', 'bionic'

                        archiveArtifacts(artifacts: "${buildDir1804}/*")

                        //---------------------------
                        // Build Ubuntu 20.04 package
                        //---------------------------
                        def buildDir2004 = 'pkg-build/2004'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir2004}"

                        sh './package 2004'

                        publishPackages buildDir2004.toString(), 'common/focal/stable', 'focal'

                        archiveArtifacts(artifacts: "${buildDir2004}/*")

                        //---------------------------
                        // Build Ubuntu 22.04 package
                        //---------------------------
                        def buildDir2204 = 'pkg-build/2204'

                        // Clean the build directory before starting
                        sh "rm -rf ${buildDir2204}"

                        sh './package 2204'

                        publishPackages buildDir2204.toString(), 'common/jammy/stable', 'jammy'

                        archiveArtifacts(artifacts: "${buildDir2204}/*")
                    }
                }
            }
        }
    }
}
