package postgresql

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	commentNameAttr     = "object_name"
	commentTypeAttr     = "object_type"
	commentDatabaseAttr = "database"
	commentCommentAttr  = "comment"
)

var commentAllowedObjectTypes = []string{
	"database",
	"table",
	"role",
}

func resourcePostgreSQLComment() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLCommentCreate),
		Read:   PGResourceFunc(resourcePostgreSQLCommentRead),
		Update: PGResourceFunc(resourcePostgreSQLCommentUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLCommentDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLCommentExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			commentNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The object upon which to comment",
			},
			commentTypeAttr: {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      false,
				ValidateFunc: validation.StringInSlice(commentAllowedObjectTypes, false),
				Description:  "The PostgreSQL object type to comment on (one of: " + strings.Join(commentAllowedObjectTypes, ", ") + ")",
			},
			commentDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "postgres",
				Description: "The database to grant privileges. Mandatory for database objects (eg. table).",
			},
			commentCommentAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     false,
				Description: "Comment to set on the object",
			},
		},
	}
}

func resourcePostgreSQLCommentCreate(db *DBConnection, d *schema.ResourceData) error {

	if !db.featureSupported(featureComment) {
		return fmt.Errorf(
			"postgresql_comment resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	if err := setComment(db, d, d.Get(commentDatabaseAttr).(string), d.Get(commentTypeAttr).(string), d.Get(commentNameAttr).(string), d.Get(commentCommentAttr).(string)); err != nil {
		return fmt.Errorf("Error creating comment: %w", err)
	}

	return resourcePostgreSQLCommentReadImpl(db, d)
}

func resourcePostgreSQLCommentExists(db *DBConnection, d *schema.ResourceData) (bool, error) {

	commentName := d.Get(commentNameAttr).(string)
	commentType := d.Get(commentTypeAttr).(string)
	commentDatabase := d.Get(commentDatabaseAttr).(string)

	description, err := getComment(db, d, commentDatabase, commentType, commentName)
	if err != nil {
		return false, err
	}

	if description != d.Get(commentCommentAttr).(string) {
		return false, nil
	}

	return true, nil
}

func resourcePostgreSQLCommentRead(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureComment) {
		return fmt.Errorf(
			"postgresql_comment resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	return resourcePostgreSQLCommentReadImpl(db, d)
}

func resourcePostgreSQLCommentReadImpl(db *DBConnection, d *schema.ResourceData) error {
	database := getDatabaseForComment(d, db.client.databaseName)

	commentName := d.Get(commentNameAttr).(string)
	commentType := d.Get(commentTypeAttr).(string)
	commentDatabase := d.Get(commentDatabaseAttr).(string)

	description, err := getComment(db, d, commentDatabase, commentType, commentName)
	if err != nil {
		return err
	}

	d.Set(commentNameAttr, d.Get(commentNameAttr))
	d.Set(commentTypeAttr, d.Get(commentTypeAttr))
	d.Set(commentCommentAttr, description)
	resourceId := generateCommentID(d, database)
	d.SetId(resourceId)

	return nil
}

func resourcePostgreSQLCommentDelete(db *DBConnection, d *schema.ResourceData) error {
	return nil
	if !db.featureSupported(featureComment) {
		return fmt.Errorf(
			"postgresql_comment resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	if err := setComment(db, d, d.Get(commentDatabaseAttr).(string), d.Get(commentTypeAttr).(string), d.Get(commentNameAttr).(string), ""); err != nil {
		return fmt.Errorf("Error deleting comment: %w", err)
	}

	d.SetId("")

	return nil
}

func resourcePostgreSQLCommentUpdate(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featureComment) {
		return fmt.Errorf(
			"postgresql_comment resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	if err := setComment(db, d, d.Get(commentDatabaseAttr).(string), d.Get(commentTypeAttr).(string), d.Get(commentNameAttr).(string), d.Get(commentCommentAttr).(string)); err != nil {
		return fmt.Errorf("Error updating comment: %w", err)
	}

	return resourcePostgreSQLCommentReadImpl(db, d)
}

func setComment(db *DBConnection, d *schema.ResourceData, commentDatabase string, commentType string, commentName string, commentValue string) error {

	//database := getDatabaseForComment(d, db.client.databaseName)

	var commentTypeObject string
	database := commentDatabase
	switch commentType {
	case "database":
		commentTypeObject = "DATABASE"
		database = "" // Don't need to specify database for database
	case "role":
		commentTypeObject = "ROLE"
		database = "" // Don't need to specify database for role
	case "table":
		commentTypeObject = "TABLE"
	default:
		return fmt.Errorf("%s is not supported", commentType)
	}

	txn, err := startTransaction(db.client, database)
	if err != nil {
		return err
	}
	defer deferredRollback(txn)

	b := bytes.NewBufferString("COMMENT ON ")
	fmt.Fprint(b, commentTypeObject)
	fmt.Fprint(b, " ", pq.QuoteIdentifier(commentName))
	fmt.Fprint(b, " IS ", pq.QuoteLiteral(commentValue))

	sql := b.String()
	if _, err := txn.Exec(sql); err != nil {
		return err
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("Error creating comment: %w", err)
	}

	return nil
}

func getComment(db *DBConnection, d *schema.ResourceData, commentDatabase string, commentType string, commentName string) (string, error) {

	txn, err := startTransaction(db.client, commentDatabase)
	if err != nil {
		return "", err
	}
	defer deferredRollback(txn)

	var query string
	switch commentType {
	case "database":
		query = `SELECT description FROM pg_catalog.pg_shdescription WHERE objoid = (SELECT oid FROM pg_database WHERE datname = $1);`
	case "role":
		query = `SELECT description FROM pg_catalog.pg_shdescription WHERE objoid = (SELECT oid FROM pg_roles WHERE rolname = $1);`
	case "table":
		query = `SELECT description FROM pg_catalog.pg_description WHERE objoid = (SELECT oid FROM pg_class WHERE relkind = 'r' and relname = $1);`
	default:
		return "", fmt.Errorf("%s is not supported", commentType)
	}

	var description string
	err = txn.QueryRow(query, commentName).Scan(&description)

	switch {
	case err == sql.ErrNoRows:
		return "", nil
	case err != nil:
		return "", fmt.Errorf("Error reading extension: %w", err)
	}

	return description, nil
}

func getDatabaseForComment(d *schema.ResourceData, databaseName string) string {
	if v, ok := d.GetOk(extDatabaseAttr); ok {
		databaseName = v.(string)
	}
	return databaseName
}

func generateCommentID(d *schema.ResourceData, databaseName string) string {
	return strings.Join([]string{databaseName, d.Get(commentNameAttr).(string)}, ".")
}
