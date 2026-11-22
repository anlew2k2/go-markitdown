package usecase

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	defaultDownloadTimeout = 30 * time.Second
	maxHTTPRedirects       = 5
)

var (
	ErrInvalidURL = errors.New("invalid or unsupported url")
	ErrBlockedURL = errors.New("url resolves to a blocked or private address")
)

var cgnatPrefix = netip.MustParsePrefix("100.64.0.0/10")
var benchmarkPrefix = netip.MustParsePrefix("198.18.0.0/15")

// validateDownloadURL ensures only http/https URLs without userinfo are accepted.
func validateDownloadURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("%w: malformed url", ErrInvalidURL)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("%w: scheme %q is not allowed", ErrInvalidURL, u.Scheme)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%w: url userinfo is not allowed", ErrInvalidURL)
	}
	if strings.TrimSpace(u.Hostname()) == "" {
		return nil, fmt.Errorf("%w: missing hostname", ErrInvalidURL)
	}
	return u, nil
}

func isAllowedPrivateHost(host string) bool {
	allowed := strings.TrimSpace(os.Getenv("MARKITDOWN_ALLOW_PRIVATE_HOSTS"))
	if allowed == "" {
		return false
	}
	for _, entry := range strings.Split(allowed, ",") {
		if strings.EqualFold(strings.TrimSpace(entry), host) {
			return true
		}
	}
	return false
}

// isBlockedIP reports whether ip belongs to loopback, private, link-local,
// CGNAT, multicast, or unspecified ranges.
func isBlockedIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || !ip.IsGlobalUnicast() {
		return true
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	addr = addr.Unmap()
	if cgnatPrefix.Contains(addr) || benchmarkPrefix.Contains(addr) {
		return true
	}
	return false
}

func safeDialContext(allowPrivate bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid address %q", ErrInvalidURL, addr)
		}

		if ip := net.ParseIP(host); ip != nil {
			if !allowPrivate && !isAllowedPrivateHost(host) && isBlockedIP(ip) {
				return nil, fmt.Errorf("%w: %s", ErrBlockedURL, ip.String())
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		}

		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve host %q: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("%w: no ip addresses for %q", ErrInvalidURL, host)
		}

		hostAllowed := allowPrivate || isAllowedPrivateHost(host)
		var dialable []net.IPAddr
		for _, ipAddr := range ips {
			if !hostAllowed && isBlockedIP(ipAddr.IP) {
				return nil, fmt.Errorf("%w: %s resolved to %s", ErrBlockedURL, host, ipAddr.IP.String())
			}
			dialable = append(dialable, ipAddr)
		}

		var lastErr error
		for _, ipAddr := range dialable {
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ipAddr.IP.String(), port))
			if dialErr == nil {
				return conn, nil
			}
			lastErr = dialErr
		}
		return nil, lastErr
	}
}

func newSafeHTTPClient(timeout time.Duration, allowPrivate bool) *http.Client {
	if timeout <= 0 {
		timeout = defaultDownloadTimeout
	}
	transport := &http.Transport{
		Proxy:                 nil, // Ignore ambient HTTP_PROXY/HTTPS_PROXY for SSRF safety.
		DialContext:           safeDialContext(allowPrivate),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: timeout,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= maxHTTPRedirects {
				return errors.New("stopped after too many redirects")
			}
			_, err := validateDownloadURL(req.URL.String())
			return err
		},
	}
}

var defaultSafeHTTPClient = newSafeHTTPClient(defaultDownloadTimeout, false)

func doSafeRequest(ctx context.Context, client *http.Client, method, rawURL string) (*http.Response, error) {
	if _, err := validateDownloadURL(rawURL); err != nil {
		return nil, err
	}
	if client == nil {
		client = defaultSafeHTTPClient
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	return client.Do(req)
}
