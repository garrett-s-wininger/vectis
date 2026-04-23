package authn

type Kind string

const (
	KindLocalUser Kind = "human_local"
)

type Principal struct {
	LocalUserID int64
	TokenID     int64
	Username    string
	Kind        Kind
	TokenScopes []TokenScope
}

type TokenScope struct {
	Action      string
	NamespaceID int64
	Propagate   bool
}
