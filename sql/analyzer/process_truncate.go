package analyzer

import (
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func processTruncate(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("processTruncate")
	defer span.Finish()

	deletePlan, ok := n.(*plan.DeleteFrom)
	if ok {
		return deleteToTruncate(ctx, a, deletePlan)
	}
	truncatePlan, ok := n.(*plan.Truncate)
	if ok {
		_, err := validateTruncate(ctx, a, truncatePlan.DatabaseName(), truncatePlan.Child())
		if err != nil {
			return nil, err
		}
		return truncatePlan, nil
	}
	return n, nil
}

func deleteToTruncate(ctx *sql.Context, a *Analyzer, deletePlan *plan.DeleteFrom) (sql.Node, error) {
	tbl, ok := deletePlan.Child.(*plan.ResolvedTable)
	if !ok {
		return deletePlan, nil
	}
	tblName := strings.ToLower(tbl.Name())

	// auto_increment behaves differently for TRUNCATE and DELETE
	for _, col := range tbl.Schema() {
		if col.AutoIncrement {
			return deletePlan, nil
		}
	}

	dbName := ""
	tblFound := false
	//TODO: if multiple dbs have a table with the same name it's ambiguous, find a way to distinguish (will remove this pass)
	for _, db := range a.Catalog.AllDatabases() {
		dbTblNames, err := db.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}
		for _, dbTblName := range dbTblNames {
			if strings.ToLower(dbTblName) == tblName {
				if tblFound == false {
					tblFound = true
					dbName = db.Name()
				} else {
					return deletePlan, nil
				}
			}
		}
	}
	if !tblFound {
		return deletePlan, nil
	}
	dbNameLower := strings.ToLower(dbName)

	for _, db := range a.Catalog.AllDatabases() {
		triggers, err := loadTriggersFromDb(ctx, db)
		if err != nil {
			return nil, err
		}
		for _, trigger := range triggers {
			if trigger.TriggerEvent != sqlparser.DeleteStr {
				continue
			}
			triggerTblName, ok := trigger.Table.(*plan.UnresolvedTable)
			if !ok {
				// If we can't determine the name of the table that the trigger is on, we just abort to be safe
				return deletePlan, nil
			}
			if (strings.ToLower(triggerTblName.Name()) == tblName) &&
				((triggerTblName.Database == "" && db.Name() == dbName) ||
					strings.ToLower(triggerTblName.Database) == dbNameLower) {
				// An ON DELETE trigger is present so we can't use TRUNCATE
				return deletePlan, nil
			}
		}
	}

	if ok, err := validateTruncate(ctx, a, dbNameLower, tbl); ok {
		// We only check err if ok is true, as some errors won't apply to us attempting to convert from a DELETE
		if err != nil {
			return nil, err
		}
		return plan.NewTruncate(dbName, tbl), nil
	}
	return deletePlan, nil
}

// validateTruncate returns whether the truncate operation adheres to the limitations as specified in
// https://dev.mysql.com/doc/refman/8.0/en/truncate-table.html. In the case of checking if a DELETE may be converted
// to a TRUNCATE operation, check the bool first. If false, then the error should be ignored (such as if the table does
// not support TRUNCATE). If true is returned along with an error, then the error is not expected to happen under
// normal circumstances and should be dealt with.
func validateTruncate(ctx *sql.Context, a *Analyzer, dbName string, tbl sql.Node) (bool, error) {
	truncatable, err := plan.GetTruncatable(tbl)
	if err != nil {
		return false, err // false as any caller besides Truncate would not care for this error
	}
	tableName := strings.ToLower(truncatable.Name())
	if dbName == "" {
		dbName = strings.ToLower(ctx.GetCurrentDatabase())
	} else {
		dbName = strings.ToLower(dbName)
	}

	for _, db := range a.Catalog.AllDatabases() {
		//TODO: when foreign keys can reference tables across databases, update this
		if strings.ToLower(db.Name()) != dbName {
			continue
		}

		tableNames, err := db.GetTableNames(ctx)
		if err != nil {
			return true, err // true as this should not error under normal circumstances
		}
		for _, tableNameToCheck := range tableNames {
			if strings.ToLower(tableNameToCheck) == tableName {
				continue
			}
			tableToCheck, ok, err := db.GetTableInsensitive(ctx, tableNameToCheck)
			if err != nil {
				return true, err // should not error under normal circumstances
			}
			if !ok {
				return true, sql.ErrTableNotFound.New(tableNameToCheck)
			}
			fkTable, ok := tableToCheck.(sql.ForeignKeyTable)
			if ok {
				fks, err := fkTable.GetForeignKeys(ctx)
				if err != nil {
					return true, err
				}
				for _, fk := range fks {
					if strings.ToLower(fk.ReferencedTable) == tableName {
						return false, sql.ErrTruncateReferencedFromForeignKey.New(tableName, fk.Name, tableNameToCheck)
					}
				}
			}
		}
	}
	//TODO: check for an active table lock and error if one is found for the target table
	return true, nil
}
