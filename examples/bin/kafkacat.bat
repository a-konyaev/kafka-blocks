@echo off
setlocal

rem see https://github.com/edenhill/kafkacat

rem for checking docker network execute commaned "docker inspect <container-name>", for example:
rem > docker inspect kafka
rem and find "Networks" section
set DOCKER_NETWORK=bin_default

rem hostname 'kafka' must be equal to kafka service name in docker-compose.yaml
set KAFKA_BROKER=kafka:9092

echo (connect to kafka broker '%KAFKA_BROKER%' on docker network '%DOCKER_NETWORK%')
echo ...

rem mount current folder to /home: -v %cd%:/home
rem echo args: %*
docker run -v %cd%:/home --network %DOCKER_NETWORK% --tty --interactive --rm confluentinc/cp-kafkacat kafkacat -b %KAFKA_BROKER% %*

rem %* - the arguments passed to this script

rem to see all possible arguments run this script without ones and you will get kafkacat help

rem arguments example:
rem "-C -o end -t <topic>" - consume events from the topic end (upcoming events)
rem "-L" - list all topics
