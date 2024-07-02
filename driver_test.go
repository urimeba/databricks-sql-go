package dbsql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urimeba/databricks-sql-go/auth/pat"
	"github.com/urimeba/databricks-sql-go/internal/config"
)

func TestOOpenConnector(t *testing.T) {
	t.Run("Should work with valid dsn", func(t *testing.T) {
		host := "databricks-host"
		port := 443
		accessToken := "token"
		httpPath := "http-path"
		expectedUserConfig := config.UserConfig{
			Host:          host,
			Port:          port,
			Protocol:      "https",
			AccessToken:   accessToken,
			HTTPPath:      "/" + httpPath,
			Authenticator: &pat.PATAuth{AccessToken: accessToken},
		}
		expectedCfg := config.WithDefaults()
		expectedCfg.UserConfig = expectedUserConfig.WithDefaults()
		expectedCfg.DriverVersion = DriverVersion
		d := &DatabricksDriver{}
		c, err := d.OpenConnector(fmt.Sprintf("token:%s@%s:%d/%s", accessToken, host, port, httpPath))
		require.NoError(t, err)
		coni, ok := c.(*connector)
		require.True(t, ok)
		assert.Equal(t, expectedCfg, coni.cfg)
		assert.NotNil(t, coni.client)
	})

}
