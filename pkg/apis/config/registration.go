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

package config

import "knative.dev/pkg/configmap"

// Constructor should take the form func(*v1.ConfigMap) (T, error)
type Constructor interface{}

// Registration defines a ConfigMap registration to be watched by the settingsstore.Watcher
// and to be injected into the Reconcile() contexts of controllers
type Registration struct {
	ConfigMapName string
	Constructor   Constructor
	Default       interface{}
}

func (r Registration) ValidateOrDie() {
	if r.ConfigMapName == "" {
		panic("ConfigMap cannot be empty in SettingsStore registration")
	}
	if err := configmap.ValidateConstructor(r.Constructor); err != nil {
		panic("Constructor validation failed in SettingsStore registration")
	}
	if r.Default == nil {
		panic("Default value cannot be empty in SettingsStore registration")
	}
}
