// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package etcd3topo

import (
	"fmt"

	"github.com/tiglabs/baudengine/topo"
)

// EtcdVersion is etcd's idea of a version.
// It implements topo.Version.
// We use the native etcd version type, int64.
type EtcdVersion int64

// String is part of the topo.Version interface.
func (v EtcdVersion) String() string {
	return fmt.Sprintf("%v", int64(v))
}

// VersionFromInt is used by old-style functions to create a proper
// Version: if version is -1, returns nil. Otherwise returns the
// EtcdVersion object.
func VersionFromInt(version int64) topo.Version {
	if version == -1 {
		return nil
	}
	return EtcdVersion(version)
}
