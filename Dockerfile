# use multi stage build, 1st stage is to create base image containing JRE only
# see example: https://blog.devops.dev/how-to-reduce-jvm-docker-image-size-by-at-least-60-459ec87b95d8

FROM --platform=linux/amd64 eclipse-temurin:21-alpine as temurin-jdk

# required for strip-debug to work
RUN apk add --no-cache binutils

# Build small JRE image
RUN jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /jre

FROM --platform=linux/amd64 alpine:latest

ENV JAVA_HOME=/jre
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY --from=temurin-jdk /jre $JAVA_HOME

LABEL maintainer="SIDN Labs"

WORKDIR /app

# Copy the application code to the container
COPY target/entrada2-*.jar /app/entrada2.jar

# Set the entrypoint command
CMD ["java", "-jar", "/app/entrada2.jar"]

