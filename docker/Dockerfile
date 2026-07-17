FROM maven:3.9-eclipse-temurin-25 AS build-base
WORKDIR /app

COPY docker/settings.xml /root/.m2/settings.xml
COPY pom.xml /app/pom.xml
COPY src /app/src

RUN mvn clean package -DskipTests
RUN cp target/janus.jar /app/janus.jar

FROM eclipse-temurin:25-jre-alpine AS runtime
WORKDIR /app

# Run as a non-root user rather than the image's default root.
RUN addgroup -S janus && adduser -S -G janus janus

COPY --from=build-base --chown=janus:janus /app/janus.jar /app/

USER janus

ENTRYPOINT ["java", "-jar", "janus.jar"]
