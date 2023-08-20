package config

import (
	"github.com/NpoolPlatform/go-service-framework/pkg/config"
)

func SupportSubsystem(system string) bool {
	subsystems := config.GetStringSliceValueWithNameSpace("", config.KeySubsystems)
	for _, subsystem := range subsystems {
		if system == subsystem {
			return true
		}
	}
	return false
}

func Subsystems() []string {
	return config.GetStringSliceValueWithNameSpace("", config.KeySubsystems)
}
