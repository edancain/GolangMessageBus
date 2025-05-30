A Message Bus implementation that facilitates publishers being able to push messages to subscribers.

- All Subscriptions are called for each message published for a given topic.
- A given topic can have multiple publishers.
- All MessageBus and Publisher functions can be called from different goroutines.
- Publishers for a topic can be created at any time
- Subscriptions to a topic can be made at any time
- MessageBus Stats may be called at any time
- Publishing should not be affected by subscribers
- Subscribers for a given topic should not affect each other
- Publishers for different topics should not affect each other


To run the code and unit tests:


To run unit tests:
Unit tests: To run an individual test, ie datadictionary:
go test ./bus/datadictionary -v  

All tests:
go test ./...

I have created a simulation test (telemetry_simulation_test.go) for telemetry data from a rocket. 

The TestRocketTelemetrySimulation function is an integration test for the message bus system. It simulates a very basic high-frequency telemetry data stream from a rocket. It tests that the message bus is functioning as expected. NB: This test does not introduce errors nor does it simulate different sensor rates, i.e IMU @ 1000Hz and GPS at 1Hz.


The code within the file telemetry_simulation_test.go, please run:

go test ./bus -v -run TestRocketTelemetrySimulation


In the test message topics (latitude, longitude, altitude, temperature, fuel-level, velocity, atmospheric_pressure, air_density, orientation) are used.
Tests:
Ordered Delivery: Messages are being delivered in the correct order, and should be evidenced by the sequential data point numbers in debug logs.
The DataDictionary should store and retrieve messages for each topic. Each topic has exactly 1000 messages, this is configurable: messageCount := 1000.
Subscription Handling: All subscribers (9 (4 for position, 2 for status, 3 for environment)) should receive messages.
Performance: The test simulates 1000 messages for 9 topics (9000 total messages).


Note to self, all code passes formatting and linting:
go fmt ./...
go vet ./...
golangci-lint run


Development ideas:
Time To Live (TTL):
Specifying a TTL allows the system to limit the validity period of the message. If a consumer does not process it in due time, it is automatically removed from the queue (and transferred to a dead-letter exchange; read more on that later). TTL is beneficial for time-sensitive commands that become irrelevant after some time has passed without processing.
Dead Letter Exchange
Throttling. America's Cup scenario. Throttle the speed of which consumers recieved publisher data, as use with teams and media at AC37
