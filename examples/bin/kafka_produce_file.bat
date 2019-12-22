@echo off
setlocal

set TOPIC=position

set file=%1
call set "file=%%file:\=/%%"

echo produce events to topic '%TOPIC%' from file '%file%'
.\kafkacat.bat -t "%TOPIC%" -P -K\t -l /home/%file%
