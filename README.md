# RocketLab

To run unit tests:


The code within the file telemetry_simulation_test.go, please run:
go test ./bus -v -run TestRocketTelemetrySimulation

The TestRocketTelemetrySimulation function is an integration test for the message bus system. It simulates a very basic high-frequency telemetry data stream from a rocket. It tests that the message bus is functioning as expected. 

In the test message topics (latitude, longitude, altitude, temperature, fuel-level, velocity, atmospheric_pressure, air_density, orientation) are used.
Ordered Delivery: Messages are being delivered in the correct order, and should be evidenced by the sequential data point numbers in debug logs.
The DataDictionary should store and retrieve messages for each topic. Each topic has exactly 1000 messages, this is configurable: messageCount := 1000.
Subscription Handling: All subscribers (9 (4 for position, 2 for status, 3 for environment)) should receive messages.
Performance: The test simulates 1000 messages for 9 topics (9000 total messages).

Unit tests: To run an individual test, ie datadictionary:
go test ./bus/datadictionary -v  


All code passes:
go fmt ./...
go vet ./...
golangci-lint run