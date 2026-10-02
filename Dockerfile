# syntax=docker/dockerfile:1
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:21-jre
RUN useradd -r -u 10001 app
WORKDIR /app
COPY --from=build /src/target/seat-reservation.jar /app/app.jar
COPY burst /app/burst
USER app
ENV PORT=8080
# Heap is a share of the container's memory limit; the rest covers thread
# stacks, metaspace, code cache and socket buffers. Measured: a 20k burst
# peaks at ~500 MB RSS with a 256 MB heap.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=50 -XX:+UseSerialGC -Xss256k -XX:ReservedCodeCacheSize=64m -XX:MaxMetaspaceSize=128m -XX:MaxDirectMemorySize=64m -XX:+ExitOnOutOfMemoryError"
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
