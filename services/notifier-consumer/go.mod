module github.com/fathurwithyou/microservices/services/notifier-consumer

go 1.26.0

require (
	github.com/fathurwithyou/microservices/contracts v0.0.0
	github.com/rabbitmq/amqp091-go v1.10.0
	google.golang.org/protobuf v1.36.10
)

replace github.com/fathurwithyou/microservices/contracts => ../../contracts
