rem format:
rem docker logs [-f] [--tail <number of rows>] <container name>

rem -f  - reading logs in stream mode

rem reading last 100 rows from "examples" container in stream mode
docker logs -f --tail 100 examples
