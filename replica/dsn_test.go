package replica

import (
	"database/sql"
	"path/filepath"
	"testing"
)

// TestDSNAppliesTheDurabilityProfile opens a real file and asks the database
// what it got. The DSN string is exactly what differs between the two backends —
// each ignores the other's spelling — so asserting on the string would pass on
// the build that ignores it.
func TestDSNAppliesTheDurabilityProfile(t *testing.T) {
	db, err := sql.Open("sqlite", dsn(filepath.Join(t.TempDir(), "t.db")))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	for _, c := range []struct{ pragma, want string }{
		{"journal_mode", "wal"},
		{"foreign_keys", "1"},
		{"busy_timeout", "10000"},
	} {
		var got string
		if err := db.QueryRow("PRAGMA " + c.pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", c.pragma, err)
		}
		if got != c.want {
			t.Errorf("%s = %q, want %q — the DSN asked for it and the backend did not take it", c.pragma, got, c.want)
		}
	}
}
