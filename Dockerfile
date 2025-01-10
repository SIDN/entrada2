FROM eclipse-temurin:23 AS jre-build

# Create a custom Java runtime
RUN $JAVA_HOME/bin/jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime

# use Debian slim as base image
FROM debian:bookworm-slim

# add the JDK to the base image
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV JAVA_OPTS=""

COPY --from=jre-build /javaruntime $JAVA_HOME

RUN mkdir /app

# Copy the application code to the container
COPY target/entrada2-*.jar /app/entrada2.jar

# use entrypoint to make sure JAVA_OPTS is passed correctly to the JVM when starting the container
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


ENTRYPOINT ["/entrypoint.sh"]
