package kafka

// Topic names. Each topic also has a dead-letter topic named Topic + ".dlq".
const (
	TopicInventoryCommands = "inventory.commands"
	TopicInventoryEvents   = "inventory.events"
	TopicPaymentCommands   = "payment.commands"
	TopicPaymentEvents     = "payment.events"
	TopicOrderEvents       = "order.events"
)

// Topics lists every topic. Each one's dead-letter topic is DLQTopic(t).
var Topics = []string{
	TopicInventoryCommands,
	TopicInventoryEvents,
	TopicPaymentCommands,
	TopicPaymentEvents,
	TopicOrderEvents,
}

// DLQTopic returns the dead-letter topic for topic.
func DLQTopic(topic string) string { return topic + dlqSuffix }

const dlqSuffix = ".dlq"
