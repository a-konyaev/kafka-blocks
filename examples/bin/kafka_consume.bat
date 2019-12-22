@echo off

set TOPIC=position

echo consume events from topic '%TOPIC%'

rem consume from end (newly incoming events)
.\kafkacat.bat -t "%TOPIC%" -C -o end -K\t

rem consume from beginning and print key
rem .\kafkacat.bat -t "%TOPIC%" -C -o beginning -K\t 
