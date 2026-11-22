// Copyright 2025 Mehmet ERGULCU
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"testing"

	"github.com/ergulcu/schedu-cloud/internal/store"
	"github.com/ergulcu/schedu-cloud/internal/store/storetest"
)

func TestStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return New() })
}
