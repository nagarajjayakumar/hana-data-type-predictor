FROM yogeshprabhu/myhana

COPY pom.xml naarai/

COPY Jenkinsfile naarai/

COPY src/ naarai/src/

COPY dev/ naarai/dev/

COPY config naarai/config

WORKDIR naarai/

RUN  mvn install:install-file -Dfile=/opt/spark/work-dir/pipeline/target/hana-spark-connector-1.0-SNAPSHOT.jar -DpomFile=/opt/spark/work-dir/pipeline/pom.xml

RUN ["mvn", "install", "-Dmaven.test.skip=true"]

ENTRYPOINT [ "/bin/bash" ]
