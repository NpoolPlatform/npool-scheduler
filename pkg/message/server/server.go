package server

import (
	msg "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/message"
	msgsrv "github.com/NpoolPlatform/go-service-framework/pkg/rabbitmq/server"
)

func Init() error {
	return msg.InitQueues()
}

func PublishExample(example *msg.Example) error {
	return msgsrv.PublishToQueue(msg.QueueExample, example)
}
