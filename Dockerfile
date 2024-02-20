FROM docker.io/maven:3-eclipse-temurin-21 as build
COPY $PWD /openehr-obds
WORKDIR /openehr-obds
RUN mvn -DskipTests clean package

FROM gcr.io/distroless/java21-debian12
COPY --from=build /openehr-obds/target/openehr-obds-*-jar-with-dependencies.jar /app/openehr-obds.jar
ENTRYPOINT ["java", "-jar", "/app/openehr-obds.jar"]