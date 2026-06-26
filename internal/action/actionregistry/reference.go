package actionregistry

import sdkaction "vectis/sdk/action"

type SelectorKind = sdkaction.SelectorKind

const (
	SelectorNone    = sdkaction.SelectorNone
	SelectorVersion = sdkaction.SelectorVersion
	SelectorDigest  = sdkaction.SelectorDigest
)

type Reference = sdkaction.Reference

func ParseReference(raw string) (Reference, error) {
	return sdkaction.ParseReference(raw)
}

func ParseBuiltinReference(uses string) (Reference, error) {
	return sdkaction.ParseBuiltinReference(uses)
}

func validReferencePart(value string) bool {
	return sdkaction.ValidReferencePart(value)
}

func validSelector(value string) bool {
	return sdkaction.ValidSelector(value)
}

func validSHA256Digest(value string) bool {
	return sdkaction.ValidSHA256Digest(value)
}
