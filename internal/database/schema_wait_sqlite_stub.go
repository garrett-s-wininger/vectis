//go:build nosqlite

package database

func sqliteWaitIsConnectFailure(err error) (connectFailure bool, matched bool) {
	return false, false
}
