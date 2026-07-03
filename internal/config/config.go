package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/dotenv"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Primary Primary      `koanf:"primary"`
	Server  ServerConfig `koanf:"server"`
}

type Primary struct {
	Env    string   `koanf:"env"`
	Name   string   `koanf:"name"`   // this node's identity — hashed onto the ring
	Number int      `koanf:"number"` // metadata only
	Peers  []string `koanf:"peers"`  // all nodes as name@host:port (incl. self); ring + networking built from this
}

type ServerConfig struct {
	Port               string   `koanf:"port"`
	ReadTimeout        int      `koanf:"read_timeout"`
	WriteTimeout       int      `koanf:"write_timeout"`
	IdleTimeout        int      `koanf:"idle_timeout"`
	CORSAllowedOrigins []string `koanf:"cors_allowed_origins"`
}

// Load reads config from .env file then overrides with environment variables.
// Keys are lowercased and double-underscores map to nested koanf keys (e.g. SERVER__PORT → server.port).
func Load(path string) (*Config, error) {
	k := koanf.New(".")

	// dotenv file (optional): transform SERVER__PORT → server.port. If the file
	// is absent (e.g. in Docker, where env vars carry the config), skip it and
	// rely on env vars alone.
	if _, statErr := os.Stat(path); statErr == nil {
		if err := k.Load(file.Provider(path), dotenv.ParserEnv("", ".", func(s string) string {
			return strings.ReplaceAll(strings.ToLower(s), "__", ".")
		})); err != nil {
			return nil, fmt.Errorf("load .env file %q: %w", path, err)
		}
	}

	// env vars override the file with the same transformation
	if err := k.Load(env.Provider("", ".", func(s string) string {
		return strings.ReplaceAll(strings.ToLower(s), "__", ".")
	}), nil); err != nil {
		return nil, fmt.Errorf("load env vars: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
