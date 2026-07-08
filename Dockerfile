FROM busybox:uclibc AS wget
FROM docker.io/maven:3-eclipse-temurin-25 as build
COPY $PWD /medic-etl-toolkit
WORKDIR /medic-etl-toolkit
RUN mvn -DskipTests clean package

FROM gcr.io/distroless/java25-debian13
COPY --from=wget /bin/wget /bin/wget
COPY --from=wget /bin/sh /bin/sh
COPY --from=build /medic-etl-toolkit/target/medic-etl-toolkit-*-jar-with-dependencies.jar /app/medic-etl-toolkit.jar
ENTRYPOINT ["java", "-jar", "/app/medic-etl-toolkit.jar"]
HEALTHCHECK CMD /bin/wget -q -O - http://localhost:4567/health || exit 1