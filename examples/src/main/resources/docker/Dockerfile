FROM repo.croc.ru/ewrnd.docker/openjdk-base:8
ARG JAR_FILE
ADD revision.txt /revision.txt
ADD ${JAR_FILE} /usr/share/reports-processor.jar
CMD ["java", "-jar", "/usr/share/reports-processor.jar"]
