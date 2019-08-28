node ('git') {
    stage ('checkout') {
        checkout scm
    }

    stage ('build') {
        // Report status of stage 'build' back to Gitlab
        gitlabCommitStatus(name: 'build') {
            // Populate changelog file with version information from git tag
            dir ('debian') {
                sh './make-changelog'
            }

            //---------------------------
            // Build Ubuntu 16.04 package
            //---------------------------
            def buildDir1604 = 'pkg-build/1604'

            // Clean the build directory before starting
            sh "rm -rf ${buildDir1604}"

            sh './package 1604'
            publishPackages buildDir1604.toString(), 'common/stable', 'xenial'
            archiveArtifacts(artifacts: "${buildDir1604}/*")

            //---------------------------
            // Build Ubuntu 18.04 package
            //---------------------------
            def buildDir1804 = 'pkg-build/1804'

            // Clean the build directory before starting
            sh "rm -rf ${buildDir1804}"

            sh './package 1804'
            publishPackages buildDir1804.toString(), 'common/bionic', 'bionic'
            archiveArtifacts(artifacts: "${buildDir1804}/*")
        }
    }
}
