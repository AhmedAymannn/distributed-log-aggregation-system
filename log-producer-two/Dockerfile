From eclipse-temurin:21-jammy

WORKDIR /app

COPY target/*.jar app.jar


EXPOSE 3002

ENTRYPOINT ["java", "-jar", "app.jar"]