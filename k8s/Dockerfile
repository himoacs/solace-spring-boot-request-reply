# Runtime-only image: the jar is built on the host by Maven and copied in, which keeps the
# image small and the build reproducible without a JDK or a populated ~/.m2 inside Docker.
FROM eclipse-temurin:17-jre

WORKDIR /app
COPY booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar app.jar

# Container-aware heap sizing. Without MaxRAMPercentage the JVM takes a fraction of the *node's*
# memory rather than the pod limit, and gets OOMKilled by the kubelet long before it ever throws
# OutOfMemoryError -- a restart loop with no Java stack trace to explain it.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=70"

EXPOSE 8091

# The "$@" and the trailing "--" are load-bearing. With ENTRYPOINT ["sh","-c","<script>"], any
# args Kubernetes supplies become the script's positional parameters starting at $0 rather than
# being appended to the command -- so a plain `exec java -jar app.jar` silently DISCARDS
# `--spring.profiles.active=replier`, and the pod runs the default profile while looking healthy.
# "--" fills $0 so the real arguments start at $1, and "$@" passes them through.
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]
