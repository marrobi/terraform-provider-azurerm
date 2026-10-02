// Copyright IBM Corp. 2014, 2025
// SPDX-License-Identifier: MPL-2.0

package network

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-azure-sdk/resource-manager/network/2025-07-01/privatednszonegroups"
	"github.com/hashicorp/go-azure-sdk/resource-manager/network/2025-07-01/privateendpoints"
	"github.com/hashicorp/go-azure-sdk/sdk/client"
	"github.com/hashicorp/go-azure-sdk/sdk/environments"
)

func TestDeletePrivateDnsZoneGroupForPrivateEndpoint(t *testing.T) {
	id := privateendpoints.NewPrivateEndpointID("00000000-0000-0000-0000-000000000000", "test", "test")
	testCases := []struct {
		name           string
		groups         []string
		status         int
		code           string
		failures       int
		timeout        time.Duration
		deletes        int32
		polls          int32
		pollStatus     string
		pollHTTPStatus int
		listStatus     int
		cancelOnError  bool
		errorContains  string
	}{
		{
			name:    "duplicate group names are deleted once",
			groups:  []string{"default", "DEFAULT"},
			deletes: 1,
			polls:   1,
		},
		{
			name:    "groups are deleted sequentially and polled",
			groups:  []string{"first", "second"},
			deletes: 2,
			polls:   2,
		},
		{
			name:     "operation in progress is retried",
			groups:   []string{"default"},
			status:   http.StatusConflict,
			code:     "AnotherOperationInProgress",
			failures: 1,
			deletes:  2,
			polls:    1,
		},
		{
			name:     "already deleted group is successful",
			groups:   []string{"default"},
			status:   http.StatusNotFound,
			code:     "ResourceNotFound",
			failures: 1,
			deletes:  1,
		},
		{
			name:          "other conflicts are not retried",
			groups:        []string{"default"},
			status:        http.StatusConflict,
			code:          "CannotDeleteResource",
			failures:      1,
			deletes:       1,
			errorContains: "CannotDeleteResource",
		},
		{
			name:          "similar conflict codes are not retried",
			groups:        []string{"default"},
			status:        http.StatusConflict,
			code:          "AnotherOperationInProgressExtra",
			failures:      1,
			deletes:       1,
			errorContains: "AnotherOperationInProgressExtra",
		},
		{
			name:          "other statuses are not retried",
			groups:        []string{"default"},
			status:        http.StatusBadRequest,
			code:          "AnotherOperationInProgress",
			failures:      1,
			deletes:       1,
			errorContains: "unexpected status 400",
		},
		{
			name:          "retries stop at the operation deadline",
			groups:        []string{"default"},
			status:        http.StatusConflict,
			code:          "AnotherOperationInProgress",
			failures:      100,
			timeout:       300 * time.Millisecond,
			deletes:       1,
			errorContains: "AnotherOperationInProgress",
		},
		{
			name:          "polling errors are not masked",
			groups:        []string{"default"},
			deletes:       1,
			polls:         1,
			pollStatus:    "Failed",
			errorContains: "polling after Delete",
		},
		{
			name:           "polling not found does not bypass completion",
			groups:         []string{"default"},
			deletes:        1,
			polls:          1,
			pollHTTPStatus: http.StatusNotFound,
			timeout:        300 * time.Millisecond,
			errorContains:  "context deadline exceeded",
		},
		{
			name:          "cancellation interrupts retry backoff",
			groups:        []string{"default"},
			status:        http.StatusConflict,
			code:          "AnotherOperationInProgress",
			failures:      100,
			deletes:       1,
			cancelOnError: true,
			errorContains: "AnotherOperationInProgress",
		},
		{
			name: "no groups",
		},
		{
			name:       "missing endpoint is successful",
			listStatus: http.StatusNotFound,
		},
		{
			name:          "listing errors are not masked",
			listStatus:    http.StatusForbidden,
			errorContains: "retrieving",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			timeout := testCase.timeout
			if timeout == 0 {
				timeout = 5 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			var deletes, polls atomic.Int32
			var pending atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(client.SkipPollingDelayHeader, "true")

				switch {
				case req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/privateDnsZoneGroups"):
					if testCase.listStatus != 0 {
						w.WriteHeader(testCase.listStatus)
						return
					}
					groups := make([]map[string]string, 0, len(testCase.groups))
					for _, name := range testCase.groups {
						groupID := privatednszonegroups.NewPrivateDnsZoneGroupID(id.SubscriptionId, id.ResourceGroupName, id.PrivateEndpointName, name)
						groups = append(groups, map[string]string{"id": groupID.ID()})
					}
					if err := json.NewEncoder(w).Encode(map[string]any{"value": groups}); err != nil {
						t.Errorf("encoding groups: %v", err)
					}

				case req.Method == http.MethodDelete:
					if pending.Load() {
						t.Error("deleting a group before the previous operation completed")
					}
					if deletes.Add(1) <= int32(testCase.failures) {
						w.WriteHeader(testCase.status)
						if err := json.NewEncoder(w).Encode(map[string]any{
							"error": map[string]string{"code": testCase.code, "message": "test error"},
						}); err != nil {
							t.Errorf("encoding error: %v", err)
						}
						if testCase.cancelOnError {
							time.AfterFunc(50*time.Millisecond, cancel)
						}
						return
					}
					pending.Store(true)
					w.Header().Set("Azure-AsyncOperation", "http://"+req.Host+"/operation")
					w.WriteHeader(http.StatusAccepted)

				case req.Method == http.MethodGet && req.URL.Path == "/operation":
					polls.Add(1)
					if testCase.pollHTTPStatus != 0 {
						w.Header().Del(client.SkipPollingDelayHeader)
						w.Header().Set("Retry-After", "1")
						w.WriteHeader(testCase.pollHTTPStatus)
						return
					}
					pending.Store(false)
					status := testCase.pollStatus
					if status == "" {
						status = "Succeeded"
					}
					if err := json.NewEncoder(w).Encode(map[string]string{"status": status}); err != nil {
						t.Errorf("encoding operation: %v", err)
					}

				default:
					t.Errorf("unexpected request: %s %s", req.Method, req.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()

			dnsClient, err := privatednszonegroups.NewPrivateDnsZoneGroupsClientWithBaseURI(environments.ResourceManagerAPI(server.URL))
			if err != nil {
				t.Fatalf("building client: %v", err)
			}
			dnsClient.Client.AuthorizeRequest = nil
			dnsClient.Client.DisableRetries = true

			start := time.Now()
			err = deletePrivateDnsZoneGroupForPrivateEndpoint(ctx, dnsClient, id)
			if testCase.cancelOnError && time.Since(start) >= 400*time.Millisecond {
				t.Error("cancellation did not interrupt retry backoff")
			}
			if testCase.errorContains == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), testCase.errorContains) {
				t.Fatalf("expected error containing %q, got %v", testCase.errorContains, err)
			}
			if got := deletes.Load(); got != testCase.deletes {
				t.Errorf("expected %d deletes, got %d", testCase.deletes, got)
			}
			if got := polls.Load(); got != testCase.polls {
				t.Errorf("expected %d polls, got %d", testCase.polls, got)
			}
			if err == nil && pending.Load() {
				t.Error("returned before polling completed")
			}
		})
	}
}
