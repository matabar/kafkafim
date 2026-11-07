# syntax=docker/dockerfile:1
# Runtime-only image. The jar is built and tested by Maven first (CI: the `build`
# job); this file only layers it. See docs/DELIVERY.md.
ARG JAR_FILE=target/*.jar

# Extraction output is platform-independent: run it natively instead of under QEMU.
FROM --platform=$BUILDPLATFORM eclipse-temurin:27-jre AS extract
WORKDIR /build
ARG JAR_FILE
COPY ${JAR_FILE} application.jar
RUN java -Djarmode=tools -jar application.jar extract --layers --destination extracted

FROM eclipse-temurin:27-jre
LABEL org.opencontainers.image.source="https://github.com/matabar/kafkafim" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.title="kafkafim" \
      org.opencontainers.image.description="Kafka to Elasticsearch ingest for Kubernetes"
WORKDIR /app
# Layer order: least to most frequently changing, so image pushes/pulls are small.
COPY --from=extract /build/extracted/dependencies/ ./
COPY --from=extract /build/extracted/spring-boot-loader/ ./
COPY --from=extract /build/extracted/snapshot-dependencies/ ./
COPY --from=extract /build/extracted/application/ ./
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError" \
    SPRING_CONFIG_ADDITIONAL_LOCATION=optional:/etc/kafkafim/config/
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
