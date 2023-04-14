package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

var testAccPostgresqlCommentConfig = `
resource "postgresql_comment" "my_database" {
  object_type = "database"
  object_name = "my_database"
  comment     = "my database comment"
}
`

var testAccPostgresqlCommentRole = `
resource "postgresql_comment" "my_role" {
  object_type = "role"
  object_name = "demo"
  comment     = "my role comment"
}
`

var testAccPostgresqlCommentTable = `
resource "postgresql_comment" "my_table" {
  database    = "my_database"
  object_type = "table"
  object_name = "table1"
  comment     = "my table comment"
}
`

func TestAccPostgresqlComment_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureExtension)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlCommentDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlCommentConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlCommentExists("postgresql_comment.my_database"),
					resource.TestCheckResourceAttr("postgresql_comment.my_database", "object_type", "database"),
					resource.TestCheckResourceAttr("postgresql_comment.my_database", "object_name", "my_database"),
					resource.TestCheckResourceAttr("postgresql_comment.my_database", "comment", "my database comment"),
				),
			},
		},
	})
}
func TestAccPostgresqlComment_Role(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureExtension)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlCommentDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlCommentRole,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlCommentExists("postgresql_comment.my_role"),
					resource.TestCheckResourceAttr("postgresql_comment.my_role", "object_type", "role"),
					resource.TestCheckResourceAttr("postgresql_comment.my_role", "object_name", "demo"),
					resource.TestCheckResourceAttr("postgresql_comment.my_role", "comment", "my role comment"),
				),
			},
		},
	})
}

func TestAccPostgresqlComment_Table(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureExtension)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlCommentDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlCommentTable,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlCommentExists("postgresql_comment.my_table"),
					resource.TestCheckResourceAttr("postgresql_comment.my_table", "object_type", "table"),
					resource.TestCheckResourceAttr("postgresql_comment.my_table", "object_name", "table1"),
					resource.TestCheckResourceAttr("postgresql_comment.my_table", "comment", "my table comment"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlCommentDestroy(s *terraform.State) error {
	// TODO
	return nil
}

func testAccCheckPostgresqlCommentExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {

		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Resource not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		commentDatabase, ok := rs.Primary.Attributes[commentDatabaseAttr]
		commentType, ok := rs.Primary.Attributes[commentTypeAttr]
		commentName, ok := rs.Primary.Attributes[commentNameAttr]

		client := testAccProvider.Meta().(*Client)
		txn, err := startTransaction(client, commentDatabase)
		if err != nil {
			return err
		}
		defer deferredRollback(txn)

		exists, err := checkCommentExists(txn, commentType, commentName, rs.Primary.Attributes[commentCommentAttr])

		if err != nil {
			return fmt.Errorf("Error checking comment %s", err)
		}

		if !exists {
			return fmt.Errorf("Comment not found")
		}

		return nil
	}
}

func checkCommentExists(txn *sql.Tx, commentType string, commentName string, commentValue string) (bool, error) {

	var query string
	switch commentType {
	case "database":
		query = `SELECT description FROM pg_catalog.pg_shdescription WHERE objoid = (SELECT oid FROM pg_database WHERE datname = $1);`
	case "role":
		query = `SELECT description FROM pg_catalog.pg_shdescription WHERE objoid = (SELECT oid FROM pg_roles WHERE rolname = $1);`
	case "table":
		query = `SELECT description FROM pg_catalog.pg_description WHERE objoid = (SELECT oid FROM pg_class WHERE relkind = 'r' and relname = $1);`
	default:
		return false, fmt.Errorf("%s is not supported", commentType)
	}

	var description string
	err := txn.QueryRow(query, commentName).Scan(&description)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about comment: %s", err)
	}

	if description != commentValue {
		return false, nil
	}

	return true, nil
}
