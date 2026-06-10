package actionregistry

import (
	"fmt"
	"strings"
)

type DescriptorStatusError struct {
	Uses       string
	Descriptor Descriptor
	Status     DescriptorStatus
	Reason     string
}

func (e *DescriptorStatusError) Error() string {
	if e == nil {
		return "action descriptor is unavailable"
	}

	uses := strings.TrimSpace(e.Uses)
	if uses == "" {
		uses = e.Descriptor.CanonicalName
	}

	status := NormalizeDescriptorStatus(e.Status)
	reason := strings.TrimSpace(e.Reason)
	if reason == "" {
		reason = strings.TrimSpace(e.Descriptor.StatusReason)
	}

	msg := fmt.Sprintf("action %q is %s", uses, status)
	if reason != "" {
		msg += ": " + reason
	}

	return msg
}

func NormalizeDescriptorStatus(status DescriptorStatus) DescriptorStatus {
	status = DescriptorStatus(strings.TrimSpace(strings.ToLower(string(status))))
	if status == "" {
		return DescriptorStatusActive
	}

	return status
}

func ValidateDescriptorStatus(status DescriptorStatus) error {
	switch NormalizeDescriptorStatus(status) {
	case DescriptorStatusActive, DescriptorStatusYanked, DescriptorStatusRevoked, DescriptorStatusPurged:
		return nil
	default:
		return fmt.Errorf("unsupported action descriptor status %q", status)
	}
}

func ValidateDescriptorUse(uses string, descriptor Descriptor) error {
	ref, err := ParseReference(uses)
	if err != nil && descriptor.Source == SourceBuiltin {
		ref, err = ParseBuiltinReference(uses)
	}
	if err != nil {
		return err
	}

	return ValidateDescriptorUseForReference(ref, descriptor)
}

func ValidateDescriptorUseForReference(ref Reference, descriptor Descriptor) error {
	status := descriptor.LifecycleStatus()
	if err := ValidateDescriptorStatus(status); err != nil {
		return err
	}

	switch status {
	case DescriptorStatusActive:
		return nil
	case DescriptorStatusYanked:
		if ref.SelectorKind == SelectorDigest {
			return nil
		}
		return &DescriptorStatusError{
			Uses:       ref.String(),
			Descriptor: descriptor,
			Status:     status,
		}
	case DescriptorStatusRevoked, DescriptorStatusPurged:
		return &DescriptorStatusError{
			Uses:       ref.String(),
			Descriptor: descriptor,
			Status:     status,
		}
	default:
		return fmt.Errorf("unsupported action descriptor status %q", status)
	}
}

func ValidateFrozenDescriptorExecution(uses string, descriptor Descriptor) error {
	status := descriptor.LifecycleStatus()
	if err := ValidateDescriptorStatus(status); err != nil {
		return err
	}

	switch status {
	case DescriptorStatusActive, DescriptorStatusYanked:
		return nil
	case DescriptorStatusRevoked, DescriptorStatusPurged:
		return &DescriptorStatusError{
			Uses:       uses,
			Descriptor: descriptor,
			Status:     status,
		}
	default:
		return fmt.Errorf("unsupported action descriptor status %q", status)
	}
}
