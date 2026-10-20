# syntax=docker/dockerfile:1.7
# One build stage compiles every module; each service has its own small runtime stage.
#   docker build -f deploy/docker/Dockerfile --target gateway -t matchbook/gateway .

FROM maven:3.10.0-eclipse-temurin-25-noble AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -q -DskipTests -pl engine-server,gateway,marketdata,posttrade -am package

FROM eclipse-temurin:25-jre-noble AS runtime
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid 10001 --home /app app
WORKDIR /app
USER app

FROM runtime AS engine
COPY --from=build /src/engine-server/target/engine-server.jar /app/app.jar
USER root
RUN mkdir -p /data && chown app:app /data
USER app
ENV ENGINE_DATA_DIR=/data
EXPOSE 9090 9100
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-XX:+AlwaysPreTouch", "-jar", "/app/app.jar"]

FROM runtime AS gateway
COPY --from=build /src/gateway/target/gateway-*.jar /app/app.jar
EXPOSE 8080 9091 9876
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/app.jar"]

FROM runtime AS marketdata
COPY --from=build /src/marketdata/target/marketdata-*.jar /app/app.jar
EXPOSE 8081
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/app.jar"]

FROM runtime AS posttrade
COPY --from=build /src/posttrade/target/posttrade-*.jar /app/app.jar
EXPOSE 8082
ENTRYPOINT ["java", "--enable-native-access=ALL-UNNAMED", "-jar", "/app/app.jar"]
