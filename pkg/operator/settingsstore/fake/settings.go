/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fake

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"knative.dev/pkg/configmap"

	"github.com/aws/karpenter-core/pkg/apis/config"
)

var SettingsRegistration = &config.Registration{
	ConfigMapName: "karpenter-global-settings",
	Constructor:   NewFakeSettingsFromConfigMap,
	Default:       defaultSettings,
}

var defaultSettings = Settings{
	TestArg: "default",
}

type Settings struct {
	TestArg string
}

func NewFakeSettingsFromConfigMap(cm *v1.ConfigMap) (Settings, error) {
	s := defaultSettings

	if err := configmap.Parse(cm.Data,
		configmap.AsString("testArg", &s.TestArg),
	); err != nil {
		// Failing to parse means that there is some error in the Settings, so we should crash
		panic(fmt.Sprintf("parsing config data, %v", err))
	}
	return s, nil
}

func SettingsFromContext(ctx context.Context) Settings {
	data := ctx.Value(SettingsRegistration)
	if data == nil {
		// This is developer error if this happens, so we should panic
		panic("settings doesn't exist in context")
	}
	return data.(Settings)
}
