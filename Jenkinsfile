node{
  try{
  stage('Checkout'){
    git credentialsId: 'bd87436c-d48f-45be-ab8c-76f0aeda7e4d', url: 'https://github.com/arita37/spp.git'

  }

  // make sure that docker installed on your system
  // install docker plugin on jenkins "navigate 'Manage jenkins in left side', select Manage plugins, and serch for docker"
  stage('Docker Build, Push'){
    withDockerRegistry([credentialsId: '8ae230eb-56a3-425e-a1dd-19530fd195a0', url: 'https://index.docker.io/v1/']) {
      sh 'docker build -t mohamedayman/spp-python .'
      sh 'docker push mohamedayman/spp-python'
      sh 'docker rm -f  spp'
      sh 'docker run -d --rm --name spp -p 80:80   mohamedayman/spp-python:latest'
        }

    }  
    

    // run this command on your system # sudo pip install -U pytest
    // make sure the required python module installed on your system
  stage('Test'){

    sh 'py.test --verbose --junit-xml test-reports/results.xml util_spark_test.py'

  }

    
  } catch (err) {
      currentBuild.result = 'FAILURE'
    }
    finally {
        stage ('Collect test reports'){
            junit 'test-reports/results.xml'
        }
    }
        

}
