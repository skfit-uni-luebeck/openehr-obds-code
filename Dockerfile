FROM busybox:uclibc AS wget
FROM docker.io/maven:3-eclipse-temurin-25 as build
COPY $PWD /openehr-obds
WORKDIR /openehr-obds
RUN mvn -DskipTests clean package

FROM gcr.io/distroless/java25-debian13
COPY --from=wget /bin/wget /bin/wget
COPY --from=wget /bin/sh /bin/sh
COPY --from=build /openehr-obds/target/openehr-obds-*-jar-with-dependencies.jar /app/openehr-obds.jar
ENTRYPOINT ["java", "-jar", "/app/openehr-obds.jar"]
HEALTHCHECK CMD /bin/wget -q -O - http://localhost:4567/health || exit 1