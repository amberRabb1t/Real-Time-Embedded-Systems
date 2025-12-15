# Real-Time-Embedded-Systems
This project constitutes a simple producer-consumer problem. Data comes in via WebSocket and is processed in real-time before being saved.
The data source used is OKX, which provides a WebSocket API with multiple public channels for receiving cryptocurrency trade data in a constant stream.

Meant to run for long periods of time on low-power devices, in this case a RaspberryPi Zero 2 W, while not falling short of the real-time constraints.
As such, the focus is on both reliability (e.g. disruption to the network should not cause a fatal error but merely reconnection attempts) and efficiency (e.g. multithreading, prudent memory usage).

Alternate links for the libwebsockets API documentation references, in case libwebsockets.org is down:
-  Referenced in the report:  https://github.com/warmcat/libwebsockets/blob/main/READMEs/README.coding.md (to find what the reference is talking about, CTRL+F "single")
-  Referenced in a code comment:  https://github.com/warmcat/libwebsockets/blob/main/minimal-examples-lowlevel/ws-client/minimal-ws-client/minimal-ws-client.c
