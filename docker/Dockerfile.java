FROM eclipse-temurin:21-jdk-alpine AS build

WORKDIR /app
COPY pom.xml .
RUN apk add --no-cache maven
RUN mvn dependency:go-offline -B

COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
COPY --from=build /app/target/kstream-lake-1.0.0.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
    KAFKA_APP_ID="kstream-lake-app"

ENTRYPOINT ["java", "-jar", "app.jar"]
