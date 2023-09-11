package config

import (
	"sync"

	"github.com/NpoolPlatform/go-service-framework/pkg/config"
)

var localSubsystems sync.Map

func SupportSubsystem(system string) bool {
	if val, ok := localSubsystems.Load(system); ok {
		return val.(bool)
	}
	subsystems := config.GetStringSliceValueWithNameSpace("", config.KeySubsystems)
	for _, subsystem := range subsystems {
		if system == subsystem {
			return true
		}
	}
	return false
}

func Subsystems() []string {
	systems := config.GetStringSliceValueWithNameSpace("", config.KeySubsystems)
	localSubsystems.Range(func(key, value interface{}) bool {
		for _, system := range systems {
			if key.(string) == system {
				return true
			}
		}
		systems = append(systems, key.(string))
		return true
	})
	return systems
}

func EnableSubsystem(system string) {
	localSubsystems.Store(system, true)
}

func DisableSubsystem(system string) {
	localSubsystems.Store(system, false)
}
