FROM yogeshprabhu/myhana

COPY pom.xml naarai/

COPY Jenkinsfile naarai/

COPY src/ naarai/src/

COPY dev/ naarai/dev/

COPY config naarai/config

WORKDIR naarai/

RUN  mvn install:install-file -Dfile=/pipeline/target/hana-spark-connector-1.0-SNAPSHOT.jar -DpomFile=/pipeline/hana-spark-connector/pom.xml

RUN ["mvn", "install", "-Dmaven.test.skip=true"]

ENTRYPOINT [ "/bin/bash" ]
