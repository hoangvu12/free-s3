package config

import (
	"strings"
	"testing"
)

func TestLoadRequiredFields(t *testing.T) {
	base := map[string]string{
		"S3_ACCESS_KEY_ID":     "k",
		"S3_SECRET_ACCESS_KEY": "s",
	}

	cases := []struct {
		name    string
		env     map[string]string
		wantErr string // substring; "" means must succeed
		check   func(t *testing.T, c Config)
	}{
		{
			name: "minimal required env present",
			env:  map[string]string{},
			check: func(t *testing.T, c Config) {
				if c.AccessKeyID != "k" || c.SecretAccessKey != "s" {
					t.Fatalf("s3 creds not parsed: %+v", c)
				}
				// Defaults.
				if c.ReplicationFactor != 3 {
					t.Fatalf("default R = %d, want 3", c.ReplicationFactor)
				}
				if c.ChunkSize != 80<<20 {
					t.Fatalf("default chunk size = %d, want %d", c.ChunkSize, 80<<20)
				}
			},
		},
		{
			name:    "missing S3_ACCESS_KEY_ID rejected",
			env:     map[string]string{"S3_ACCESS_KEY_ID": ""},
			wantErr: "S3_ACCESS_KEY_ID",
		},
		{
			name:    "missing S3_SECRET_ACCESS_KEY rejected",
			env:     map[string]string{"S3_SECRET_ACCESS_KEY": ""},
			wantErr: "S3_SECRET_ACCESS_KEY",
		},
		{
			name: "freehost providers parsed",
			env:  map[string]string{"FREEHOST_PROVIDERS": "ia, fileditch ,catbox"},
			check: func(t *testing.T, c Config) {
				want := []string{"ia", "fileditch", "catbox"}
				if len(c.FreehostProviders) != len(want) {
					t.Fatalf("providers=%v want %v", c.FreehostProviders, want)
				}
				for i := range want {
					if c.FreehostProviders[i] != want[i] {
						t.Fatalf("providers=%v want %v", c.FreehostProviders, want)
					}
				}
			},
		},
		{
			name: "replication factor + chunk size overrides parsed",
			env:  map[string]string{"REPLICATION_FACTOR": "2", "CHUNK_SIZE": "10MiB"},
			check: func(t *testing.T, c Config) {
				if c.ReplicationFactor != 2 {
					t.Fatalf("R=%d want 2", c.ReplicationFactor)
				}
				if c.ChunkSize != 10<<20 {
					t.Fatalf("chunk=%d want %d", c.ChunkSize, 10<<20)
				}
			},
		},
		{
			name: "optional provider credentials captured",
			env: map[string]string{
				"CATBOX_USERHASH":   "abc123",
				"PIXELDRAIN_API_KEY": "pdkey",
				"IA_ACCESS_KEY":     "iak",
				"IA_SECRET_KEY":     "ias",
				"GOFILE_TOKEN":      "gft",
			},
			check: func(t *testing.T, c Config) {
				if c.CatboxUserhash != "abc123" || c.PixeldrainAPIKey != "pdkey" ||
					c.IAAccessKey != "iak" || c.IASecretKey != "ias" || c.GofileToken != "gft" {
					t.Fatalf("provider creds not captured: %+v", c)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range base {
				t.Setenv(k, v)
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, err := Load()
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err=%v want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}
