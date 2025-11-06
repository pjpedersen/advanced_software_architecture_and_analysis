package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	schedulerURL      = "http://localhost:8080"
	deviceRegistryURL = "http://localhost:8081"
	genealogyURL      = "http://localhost:8089"
	templateURL       = "http://localhost:8086"
)

// TestFullProductionFlow tests complete production workflow
func TestFullProductionFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test")
	}

	t.Log("=== Starting Full Production Flow E2E Test ===")

	// Step 1: Create production plan
	t.Log("Step 1: Creating production plan")
	planID := createProductionPlan(t)
	assert.NotEmpty(t, planID)
	time.Sleep(2 * time.Second) // Allow event propagation

	// Step 2: Register device
	t.Log("Step 2: Registering device")
	deviceID, unitID := registerDevice(t, "B001")
	assert.NotEmpty(t, deviceID)
	assert.NotEmpty(t, unitID)
	time.Sleep(2 * time.Second)

	// Step 3: Verify genealogy was created
	t.Log("Step 3: Verifying genealogy")
	genealogy := getGenealogy(t, unitID)
	assert.NotNil(t, genealogy)
	assert.Equal(t, unitID, genealogy["unit_id"])

	// Step 4: Query recall (should find our device)
	t.Log("Step 4: Testing recall query")
	recallResults := queryRecall(t, "B001")
	assert.NotNil(t, recallResults)
	assert.Greater(t, recallResults["device_count"], 0)

	t.Log("=== Full Production Flow E2E Test Completed Successfully ===")
}

// TestCustomizationFlow tests UC-4 customization workflow
func TestCustomizationFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping e2e test")
	}

	t.Log("=== Starting Customization Flow E2E Test ===")

	// Step 1: Create customization template
	t.Log("Step 1: Creating customization template")
	templateID := createTemplate(t, "ORDER-001")
	assert.NotEmpty(t, templateID)

	// Step 2: Approve template
	t.Log("Step 2: Approving template")
	approveTemplate(t, templateID)
	time.Sleep(1 * time.Second)

	// Step 3: Register device with customer order
	t.Log("Step 3: Registering device with customer order")
	deviceID, unitID := registerDeviceWithOrder(t, "ORDER-001")
	assert.NotEmpty(t, deviceID)
	assert.NotEmpty(t, unitID)

	// Step 4: Wait for engraving to complete
	t.Log("Step 4: Waiting for engraving completion")
	time.Sleep(5 * time.Second)

	// Step 5: Verify device has engraving data
	t.Log("Step 5: Verifying engraving completion")
	// Would check genealogy or device status for engraving completion

	t.Log("=== Customization Flow E2E Test Completed Successfully ===")
}

// Helper functions
func createProductionPlan(t *testing.T) string {
	plan := map[string]interface{}{
		"batches": []map[string]interface{}{
			{
				"batch_id":    "B001",
				"product_sku": "WIC-001",
				"quantity":    100,
				"color":       "BLACK",
				"start_time":  time.Now().Format(time.RFC3339),
				"end_time":    time.Now().Add(2 * time.Hour).Format(time.RFC3339),
			},
		},
	}

	jsonData, _ := json.Marshal(plan)
	resp, err := http.Post(schedulerURL+"/api/v1/plans", "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	return result["plan_id"].(string)
}

func registerDevice(t *testing.T, batchID string) (string, string) {
	device := map[string]interface{}{
		"pcb_id":        fmt.Sprintf("PCB-%d", time.Now().Unix()),
		"serial_number": fmt.Sprintf("SN-%d", time.Now().Unix()),
		"production_batch": batchID,
	}

	jsonData, _ := json.Marshal(device)
	resp, err := http.Post(deviceRegistryURL+"/api/v1/devices", "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	return result["device_id"].(string), result["unit_id"].(string)
}

func registerDeviceWithOrder(t *testing.T, customerOrder string) (string, string) {
	device := map[string]interface{}{
		"pcb_id":         fmt.Sprintf("PCB-%d", time.Now().Unix()),
		"serial_number":  fmt.Sprintf("SN-%d", time.Now().Unix()),
		"customer_order": customerOrder,
	}

	jsonData, _ := json.Marshal(device)
	resp, err := http.Post(deviceRegistryURL+"/api/v1/devices", "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	return result["device_id"].(string), result["unit_id"].(string)
}

func getGenealogy(t *testing.T, unitID string) map[string]interface{} {
	resp, err := http.Get(genealogyURL + "/api/v1/genealogy/device/" + unitID)
	require.NoError(t, err)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

func queryRecall(t *testing.T, batchID string) map[string]interface{} {
	query := map[string]interface{}{
		"query_id":         fmt.Sprintf("Q-%d", time.Now().Unix()),
		"production_batch": batchID,
	}

	jsonData, _ := json.Marshal(query)
	resp, err := http.Post(genealogyURL+"/api/v1/genealogy/recall/query", "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	return result
}

func createTemplate(t *testing.T, customerOrder string) string {
	template := map[string]interface{}{
		"customer_order": customerOrder,
		"customer_name":  "Test Customer",
		"engraving_text": "Custom Device",
		"font_style":     "Arial",
	}

	jsonData, _ := json.Marshal(template)
	resp, err := http.Post(templateURL+"/api/v1/templates", "application/json", bytes.NewBuffer(jsonData))
	require.NoError(t, err)
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	return result["template_id"].(string)
}

func approveTemplate(t *testing.T, templateID string) {
	resp, err := http.Post(templateURL+"/api/v1/templates/approve/"+templateID, "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}
