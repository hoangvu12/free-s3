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

// defaultProviderOrder is the verified-good set from live smoke testing
// (2026-07-01, from a residential IP), in read-priority order. Every host here
// passed an upload / full-GET / range / delete round-trip. Hosts that failed are
// kept in the registry (opt back in via FREEHOST_PROVIDERS once fixed or
// re-verified from the deploy IP) but are NOT in the default:
//   - dead regardless of IP: envs.sh & cockfile (DNS gone), pomf.lain.la
//     (discontinued, 404), tmp.ninja (404), ttm.sh (POST now returns HTML)
//   - blocked from this IP (re-test on the VPS): paste.c-net.org (whole-site 403
//     Blacklisted), doko.moe (upload connection reset)
//   - opt-in-only (token-gated / docs-derived): gofile, buzzheavier
// (fars.ee was dropped entirely: it is a ptpb/pb text pastebin that forbids
// large files, not a file host.)
// temp.sh and filebin.net initially served HTML on read; their adapters now do
// the POST-to-download (temp.sh) / verified-cookie 302 (filebin) dance and pass.
//
// Ordering rationale: pixeldrain is the fastest clean direct link, so it leads;
// fileditch is permanent/no-auth (landing-page scrape on read); IA is the
// permanent anchor but ingests asynchronously (a just-uploaded file 404s
// briefly), so it sits at 3 rather than 0 so fresh reads don't eat a failed IA
// fetch first; catbox/litterbox work but are bandwidth-throttled, so they trail
// as durable backups rather than primary read sources.
var defaultProviderOrder = []string{
	"pixeldrain",   // durable, fast, clean direct link
	"fileditch",    // durable, 100GB, no auth (landing-page scrape on read)
	"ia",           // permanent anchor (if credentialed; ingestion delay on read)
	"x0.at",        // durable, DC-friendly
	"catbox",       // durable but throttled (needs userhash from VPS)
	"uguu",         // temp/overflow, fast
	"tmpfiles.org", // temp/overflow
	"tmpfile.link", // temp/overflow
	"temp.sh",      // temp/overflow, 4GB (POST-to-download)
	"filebin.net",  // temp/overflow, ~6d (verified-cookie 302 download)
	"litterbox",    // temp, throttled (catbox infra) — backup
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
