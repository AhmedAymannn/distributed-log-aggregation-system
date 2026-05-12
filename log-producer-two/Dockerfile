# STAGE 1: Build
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the pom.xml from the CURRENT folder (e.g., log-producer-one/)
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy the src folder (contains your java code, application.yml, and banner.txt)
COPY src ./src

# Build the JAR - Maven automatically puts everything in src/main/resources into the JAR
RUN mvn clean package -DskipTests

# STAGE 2: Run
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Copy the JAR from the build stage to the final image
COPY --from=build /app/target/*.jar app.jar

EXPOSE 3002
ENTRYPOINT ["java", "-jar", "app.jar"]