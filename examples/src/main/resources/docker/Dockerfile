FROM adoptopenjdk/openjdk11:alpine-jre
ARG JAR_FILE
ADD ${JAR_FILE} /usr/share/examples.jar
CMD ["java", "-jar", "/usr/share/examples.jar"]
