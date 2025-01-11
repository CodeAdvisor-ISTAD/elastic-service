#FROM gradle:8.4-jdk21 AS builder
#
#WORKDIR /app
#
#COPY . .
#
## Ensure gradlew has execute permissions
#RUN chmod +x gradlew
#
## Build the application
#RUN ./gradlew clean build -x test
#
#FROM openjdk:21-jdk
#
#WORKDIR /app
#
#COPY --from=builder /app/build/libs/*.jar app.jar
#
#EXPOSE 8080
#
#ENTRYPOINT ["java", "-jar", "app.jar"]

FROM gradle:8.4-jdk21 AS builder

WORKDIR /app

COPY . .

# Ensure gradlew has execute permissions
RUN chmod +x gradlew

# Build the application
RUN ./gradlew clean build -x test

FROM openjdk:21-jdk

WORKDIR /app

# Copy the built JAR file
COPY --from=builder /app/build/libs/*.jar app.jar

# Copy the .jks files into the container
COPY src/main/resources/kafka.server.keystore.jks /app/kafka.server.keystore.jks
COPY src/main/resources/kafka.server.truststore.jks /app/kafka.server.truststore.jks

EXPOSE 8000

ENTRYPOINT ["java", "-jar", "app.jar"]