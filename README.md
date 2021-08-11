sparkplugbmock
--

Publish mock DDATA messages to mqtt listener:

# usage 
```bash
# in a terminal were a mqtt server is reachable
./sparkplugbmock

# in other terminal you could do one of the next ones
# The output is binary
mosquitto_sub -L mqtt://localhost:1883/spBv1.0/#

# the output is hexadecimal string
mosquitto_sub -L mqtt://localhost:1883/spBv1.0/# -F %X

# go install github.com/dubyte/sparkplugclidecoder@latest
# and you can see the values in protojson
mosquitto_sub -L mqtt://localhost:1883/spBv1.0/# -F %X | sparkplugclidecoder
```