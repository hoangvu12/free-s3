package freehost

import "log/slog"

// Credentials carries the optional per-provider secrets from config. Every
// field is optional; a provider whose required credential is missing is skipped
// at startup (logged).
type Credentials struct {
	CatboxUserhash   string
	PixeldrainAPIKey string
	IAAccessKey      string
	IASecretKey      string
	GofileToken      string
}

// defaultProviderOrder is the enabled set + priority used when FREEHOST_PROVIDERS
// is empty. Extended as more providers land (P5).
var defaultProviderOrder = []string{"fileditch", "catbox", "x0.at"}

// providerFactory builds a provider, returning ok=false when a required
// credential is missing (so the caller can skip + log it).
type providerFactory func(c *Client, creds Credentials) (Provider, bool)

// registry maps a provider name to its factory. Only implemented providers are
// listed; an unknown name in FREEHOST_PROVIDERS is skipped + logged.
var registry = map[string]providerFactory{
	"fileditch": func(c *Client, _ Credentials) (Provider, bool) { return NewFileditch(c), true },
	"x0.at":     func(c *Client, _ Credentials) (Provider, bool) { return NewX0(c), true },
	"catbox":    func(c *Client, cr Credentials) (Provider, bool) { return NewCatbox(c, cr.CatboxUserhash), true },
}

// BuildProviders constructs the enabled providers in priority order. An empty
// names list uses defaultProviderOrder. Unknown/unimplemented names and
// providers missing a required credential are skipped with a warning.
func BuildProviders(c *Client, names []string, creds Credentials, logger *slog.Logger) []Provider {
	if logger == nil {
		logger = slog.Default()
	}
	order := names
	if len(order) == 0 {
		order = defaultProviderOrder
	}
	var out []Provider
	seen := map[string]bool{}
	for _, name := range order {
		if seen[name] {
			continue
		}
		seen[name] = true
		factory, ok := registry[name]
		if !ok {
			logger.Warn("freehost: unknown or unimplemented provider, skipping", "name", name)
			continue
		}
		prov, available := factory(c, creds)
		if !available {
			logger.Warn("freehost: provider missing required credential, skipping", "name", name)
			continue
		}
		out = append(out, prov)
	}
	return out
}
