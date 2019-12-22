@echo off

set TOPIC=position

echo produce events to topic '%TOPIC%'
.\kafkacat.bat -t "%TOPIC%" -P -K\t
