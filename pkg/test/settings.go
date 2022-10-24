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

package test

import (
	"context"
	"time"

	"github.com/aws/karpenter-core/pkg/apis/config/settings"
)

type SettingsStore struct{}

func (ss SettingsStore) InjectSettings(ctx context.Context) context.Context {
	return settings.ToContext(ctx, Settings())
}

func Settings() settings.Settings {
	return settings.Settings{
		BatchMaxDuration:  time.Second * 10,
		BatchIdleDuration: time.Second,
	}
}
