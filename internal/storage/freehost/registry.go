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
// is empty. It lists the providers whose mechanics are verified (RESEARCH.md 🟢)
// and datacenter-friendly, anchored by Internet Archive. The opt-in-only set
// (gofile, buzzheavier — docs-derived/🟡, or needing a token) is in the registry
// but not the default; enable explicitly via FREEHOST_PROVIDERS once verified.
var defaultProviderOrder = []string{
	"ia",           // permanent anchor (if credentialed)
	"fileditch",    // durable, 100GB, no auth
	"pixeldrain",   // durable (keep-alive)
	"catbox",       // durable (needs userhash from VPS)
	"x0.at",        // durable, DC-friendly
	"pomf.lain.la", // durable, dedicated HW
	"paste.c-net.org",
	"temp.sh", // overflow / scratch
	"litterbox",
	"tmpfiles.org",
	"tmpfile.link",
	"filebin.net",
	"envs.sh",
	"ttm.sh",
	"fars.ee",
	"uguu",
	"tmp.ninja",
	"doko.moe",
	"cockfile",
}

// providerFactory builds a provider, returning ok=false when a required
// credential is missing (so the caller can skip + log it).
type providerFactory func(c *Client, creds Credentials) (Provider, bool)

// registry maps a provider name to its factory. Only implemented providers are
// listed; an unknown name in FREEHOST_PROVIDERS is skipped + logged.
var registry = map[string]providerFactory{
	// Anchor — only available when IA S3 keys are configured.
	"ia": func(c *Client, cr Credentials) (Provider, bool) {
		if cr.IAAccessKey == "" || cr.IASecretKey == "" {
			return nil, false
		}
		return NewIA(c, cr.IAAccessKey, cr.IASecretKey), true
	},
	// Anonymous durable hosts.
	"fileditch":   func(c *Client, _ Credentials) (Provider, bool) { return NewFileditch(c), true },
	"pixeldrain":  func(c *Client, cr Credentials) (Provider, bool) { return NewPixeldrain(c, cr.PixeldrainAPIKey), true },
	"catbox":      func(c *Client, cr Credentials) (Provider, bool) { return NewCatbox(c, cr.CatboxUserhash), true },
	"buzzheavier": func(c *Client, _ Credentials) (Provider, bool) { return NewBuzzheavier(c, ""), true },
	// gofile needs a token to yield a raw direct link.
	"gofile": func(c *Client, cr Credentials) (Provider, bool) {
		if cr.GofileToken == "" {
			return nil, false
		}
		return NewGofile(c, cr.GofileToken), true
	},
	// 0x0 family.
	"x0.at":   func(c *Client, _ Credentials) (Provider, bool) { return NewX0(c), true },
	"envs.sh": func(c *Client, _ Credentials) (Provider, bool) { return NewEnvsSh(c), true },
	"ttm.sh":  func(c *Client, _ Credentials) (Provider, bool) { return NewTtmSh(c), true },
	"fars.ee": func(c *Client, _ Credentials) (Provider, bool) { return NewFarsee(c), true },
	// pomf family.
	"pomf.lain.la": func(c *Client, _ Credentials) (Provider, bool) { return NewPomfLainLa(c), true },
	"uguu":         func(c *Client, _ Credentials) (Provider, bool) { return NewUguu(c), true },
	"tmp.ninja":    func(c *Client, _ Credentials) (Provider, bool) { return NewTmpNinja(c), true },
	"doko.moe":     func(c *Client, _ Credentials) (Provider, bool) { return NewDokoMoe(c), true },
	"cockfile":     func(c *Client, _ Credentials) (Provider, bool) { return NewCockfile(c), true },
	// temp / overflow.
	"temp.sh":         func(c *Client, _ Credentials) (Provider, bool) { return NewTempSh(c), true },
	"litterbox":       func(c *Client, _ Credentials) (Provider, bool) { return NewLitterbox(c), true },
	"tmpfiles.org":    func(c *Client, _ Credentials) (Provider, bool) { return NewTmpfiles(c), true },
	"tmpfile.link":    func(c *Client, _ Credentials) (Provider, bool) { return NewTmpfileLink(c), true },
	"filebin.net":     func(c *Client, _ Credentials) (Provider, bool) { return NewFilebin(c), true },
	"paste.c-net.org": func(c *Client, _ Credentials) (Provider, bool) { return NewPasteCNet(c), true },
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
