FROM eclipse-temurin:25-jre-noble

RUN mkdir /app

# Copy the application code to the container
COPY target/entrada2-*.jar /app/entrada2.jar

# use entrypoint to make sure JAVA_OPTS is passed correctly to the JVM when starting the container
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


ENTRYPOINT ["/entrypoint.sh"]
