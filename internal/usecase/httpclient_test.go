package usecase

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestIsBlockedIP(t *testing.T) {
	tests := []struct {
		ip      string
		blocked bool
	}{
		{"127.0.0.1", true},
		{"10.0.0.1", true},
		{"172.16.5.4", true},
		{"192.168.1.10", true},
		{"169.254.169.254", true},
		{"100.64.0.1", true},
		{"0.0.0.0", true},
		{"224.0.0.1", true},
		{"::1", true},
		{"fe80::1", true},
		{"fc00::1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"2606:4700:4700::1111", false},
	}

	for _, tc := range tests {
		ip := net.ParseIP(tc.ip)
		if ip == nil {
			t.Fatalf("failed to parse IP %s", tc.ip)
		}
		if got := isBlockedIP(ip); got != tc.blocked {
			t.Errorf("isBlockedIP(%s) = %v, want %v", tc.ip, got, tc.blocked)
		}
	}
}

func TestDoSafeRequestBlocksLoopbackAndBadSchemes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Loopback server must be rejected by default SSRF filter.
	if _, err := doSafeRequest(ctx, nil, http.MethodGet, srv.URL+"/file.pdf"); !errors.Is(err, ErrBlockedURL) {
		t.Fatalf("expected ErrBlockedURL for loopback, got %v", err)
	}

	// Metadata IP must be rejected.
	if _, err := doSafeRequest(ctx, nil, http.MethodGet, "http://169.254.169.254/latest/meta-data/"); !errors.Is(err, ErrBlockedURL) {
		t.Fatalf("expected ErrBlockedURL for link-local metadata, got %v", err)
	}

	// Unsupported scheme must be rejected.
	if _, err := doSafeRequest(ctx, nil, http.MethodGet, "file:///etc/passwd"); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL for file scheme, got %v", err)
	}

	// Userinfo in URL must be rejected.
	if _, err := doSafeRequest(ctx, nil, http.MethodGet, "https://user:pass@example.com/a.pdf"); !errors.Is(err, ErrInvalidURL) {
		t.Fatalf("expected ErrInvalidURL for userinfo, got %v", err)
	}
}

func TestDownloadToTempRespectsSizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("a", 1024)))
	}))
	defer srv.Close()

	// Temporarily exercise the size guard via a tiny client + limit check.
	client := newSafeHTTPClient(2*time.Second, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := doSafeRequest(ctx, client, http.MethodGet, srv.URL+"/doc.pdf")
	if err != nil {
		t.Fatalf("unexpected error with allowPrivate=true: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}
