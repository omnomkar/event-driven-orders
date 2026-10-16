package kafka

// Topic names. Each topic also has a dead-letter topic named Topic + ".dlq".
const (
	TopicInventoryCommands = "inventory.commands"
	TopicInventoryEvents   = "inventory.events"
	TopicPaymentCommands   = "payment.commands"
	TopicPaymentEvents     = "payment.events"
	TopicOrderEvents       = "order.events"
)
