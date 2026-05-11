From eclipse-temurin:21-jammy

WORKDIR /app

COPY target/*.jar app.jar

EXPOSE 3001

ENTRYPOINT ["java", "-jar", "app.jar"]