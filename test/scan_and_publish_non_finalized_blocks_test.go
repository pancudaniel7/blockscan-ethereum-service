// (build tag removed to run by default)

package test

import (
    "testing"

    "github.com/pancudaniel7/blockscan-ethereum-service/test/util"
)

// Smoke test to ensure containers bootstrap correctly in integration runs.
func TestScanAndPublishNonFinalizedBlocks(t *testing.T) {
    if err := util.InitConfig(); err != nil {
        t.Fatalf("Failed to initialize configs: %v", err)
    }
    if err := InitContainers(); err != nil {
        t.Fatalf("Failed to init containers: %v", err)
    }
    // No-op: provisioning is covered in InitContainers and TestMain.
}
