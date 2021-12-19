package listener

import (
	msgcli "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/client"
	msg "github.com/NpoolPlatform/cloud-hashing-staker/pkg/message/message"
	"github.com/NpoolPlatform/go-service-framework/pkg/logger"
)

func listenTemplateExample() {
	for {
		err := msgcli.ConsumeExample(func(example *msg.Example) error {
			// Call event handler in api module
			return nil
		})
		if err != nil {
			logger.Sugar().Errorf("fail to consume example: %v", err)
			return
		}
	}
}

func Listen() {
	go listenTemplateExample()
}
