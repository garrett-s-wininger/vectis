package api

const (
	adminUsernameMaxLen        = 128
	adminPasswordMaxLen        = 512
	maxJSONDocumentBodyBytes   = 64 << 10
	maxSetupCompleteBodyBytes  = maxJSONDocumentBodyBytes
	maxJobDefinitionBodyBytes  = 10 * 1024 * 1024
	maxBearerTokenBytes        = 4096
	maxChangePasswordBodyBytes = 4096
	maxUserBodyBytes           = 4096
	maxLoginBodyBytes          = 4096
)
