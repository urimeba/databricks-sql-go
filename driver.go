package dbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sql-go/internal/config"
	_ "github.com/databricks/databricks-sql-go/logger"
)

func init() {
	sql.Register("databricks", &DatabricksDriver{})
}

var DriverVersion = "1.5.7" // update version before each release

type DatabricksDriver struct{}

// Open returns a new connection to Databricks database with a DSN string.
// Use sql.Open("databricks", <dsn string>) after importing this driver package.
func (d *DatabricksDriver) Open(dsn string) (driver.Conn, error) {
	cn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return cn.Connect(context.Background())
}

// OpenConnector returns a new Connector.
// Used by sql.DB to obtain a Connector and invoke its Connect method to obtain each needed connection.
func (d *DatabricksDriver) OpenConnector(dsn string) (driver.Connector, error) {
	ucfg, err := config.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return NewConnector(withUserConfig(ucfg))
}

var _ driver.Driver = (*DatabricksDriver)(nil)
var _ driver.DriverContext = (*DatabricksDriver)(nil)
