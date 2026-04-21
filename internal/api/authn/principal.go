package authn

type Kind string

const (
	KindLocalUser Kind = "human_local"
)

type Principal struct {
	LocalUserID int64
	Username    string
	Kind        Kind
}
