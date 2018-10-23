package auth

import "gopkg.in/src-d/go-vitess.v1/mysql"

// None is a Auth method that always succeeds.
type None struct{}

// Mysql implements Auth interface.
func (n *None) Mysql() mysql.AuthServer {
	return new(mysql.AuthServerNone)
}

// Mysql implements Auth interface.
func (n *None) Allowed(user string, permission Permission) error {
	return nil
}
