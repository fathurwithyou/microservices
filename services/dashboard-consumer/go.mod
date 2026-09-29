module github.com/fathurwithyou/microservices/services/dashboard-consumer

go 1.26.0

require (
	github.com/fathurwithyou/microservices/contracts v0.0.0
	github.com/fathurwithyou/microservices/pkg v0.0.0
	google.golang.org/protobuf v1.36.11
)

require github.com/rabbitmq/amqp091-go v1.10.0 // indirect

replace github.com/fathurwithyou/microservices/contracts => ../../contracts

replace github.com/fathurwithyou/microservices/pkg => ../../pkg
