package source

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"vectis/internal/gitcmd"
)

type ManagedGitCheckoutRequest struct {
	CheckoutPath       string
	RemoteURL          string
	DefaultRef         string
	FallbackRemoteURLs []string
	Credentials        GitCredentials
}

type ManagedGitRefHydrationRequest struct {
	CheckoutPath       string
	Ref                string
	PreferredRemote    string
	FallbackRemoteURLs []string
	AuxiliaryRefs      []string
	Credentials        GitCredentials
}

// SyncManagedGitCheckout materializes or refreshes a Vectis-owned Git checkout.
func SyncManagedGitCheckout(ctx context.Context, req ManagedGitCheckoutRequest) GitCheckoutStatus {
	checkoutPath := strings.TrimSpace(req.CheckoutPath)
	defaultRef := strings.TrimSpace(req.DefaultRef)
	status := GitCheckoutStatus{
		CheckoutPath: checkoutPath,
		DefaultRef:   defaultRef,
	}

	if checkoutPath == "" {
		status.setError("missing_checkout_path", "checkout path is required")
		return status
	}

	fallbackRemoteURLs, err := normalizeManagedGitFallbackRemoteURLs(req.FallbackRemoteURLs)
	if err != nil {
		status.setError("git_fallback_remotes_invalid", err.Error())
		return status
	}

	credentialEnv, credentialCleanup, err := managedGitCredentialEnvironment(req.Credentials)
	if err != nil {
		status.setError("git_credentials_invalid", err.Error())
		return status
	}
	defer credentialCleanup()

	lock, err := acquireManagedGitWriterLock(ctx, checkoutPath)
	if err != nil {
		status.setError("git_lock_failed", err.Error())
		return status
	}

	defer func() {
		_ = lock.Close()
	}()

	info, err := os.Stat(checkoutPath)
	switch {
	case err == nil:
		status.PathExists = true
		status.PathIsDirectory = info.IsDir()
		if !info.IsDir() {
			status.setError("checkout_path_not_directory", "checkout path is not a directory")
			return status
		}

		checkoutStatus := NewGitCheckout(checkoutPath).Status(ctx, "")
		if checkoutStatus.ErrorCode != "" {
			checkoutStatus.DefaultRef = defaultRef
			if checkoutStatus.ErrorCode != "not_git_checkout" || !directoryIsEmpty(checkoutPath) {
				return checkoutStatus
			}

			if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL, credentialEnv, fallbackRemoteURLs); err != nil {
				checkoutStatus.ErrorCode = ""
				checkoutStatus.ErrorMessage = ""
				checkoutStatus.setError("git_clone_failed", err.Error())
				return checkoutStatus
			}
		} else {
			if err := configureManagedGitCheckout(ctx, checkoutPath, fallbackRemoteURLs); err != nil {
				checkoutStatus.DefaultRef = defaultRef
				checkoutStatus.setError("git_config_failed", err.Error())
				return checkoutStatus
			}

			if err := fetchManagedGitCheckout(ctx, checkoutPath, credentialEnv, defaultRef); err != nil {
				checkoutStatus.DefaultRef = defaultRef
				checkoutStatus.setError("git_fetch_failed", err.Error())
				return checkoutStatus
			}
		}
	case os.IsNotExist(err):
		if err := cloneManagedGitCheckout(ctx, checkoutPath, req.RemoteURL, credentialEnv, fallbackRemoteURLs); err != nil {
			status.setError("git_clone_failed", err.Error())
			return status
		}
	default:
		status.setError("checkout_path_unavailable", fmt.Sprintf("checkout path is unavailable: %v", err))
		return status
	}

	if err := alignManagedGitCheckoutRef(ctx, checkoutPath, defaultRef); err != nil {
		status := NewGitCheckout(checkoutPath).Status(ctx, "")
		status.DefaultRef = defaultRef
		status.setError("git_ref_update_failed", err.Error())
		return status
	}

	finalStatus := NewManagedGitCheckout(checkoutPath).Status(ctx, defaultRef)
	if finalStatus.ErrorCode == "default_ref_not_found" && fetchManagedGitMissingDefaultRef(ctx, checkoutPath, defaultRef, credentialEnv) == nil {
		finalStatus = NewManagedGitCheckout(checkoutPath).Status(ctx, defaultRef)
	}

	return finalStatus
}

// HydrateManagedGitContext fetches one safe branch, tag, or provider ref plus
// explicitly requested auxiliary refs into an existing managed checkout.
func HydrateManagedGitContext(ctx context.Context, req ManagedGitRefHydrationRequest) GitCheckoutStatus {
	return HydrateManagedGitRef(ctx, req)
}

// HydrateManagedGitRef fetches one safe branch, tag, or provider ref into an existing managed checkout.
func HydrateManagedGitRef(ctx context.Context, req ManagedGitRefHydrationRequest) GitCheckoutStatus {
	checkoutPath := strings.TrimSpace(req.CheckoutPath)
	ref := strings.TrimSpace(req.Ref)
	status := GitCheckoutStatus{
		CheckoutPath: checkoutPath,
		DefaultRef:   ref,
	}

	if checkoutPath == "" {
		status.setError("missing_checkout_path", "checkout path is required")
		return status
	}

	if ref == "" {
		status.setError("missing_ref", "ref is required")
		return status
	}

	fallbackRemoteURLs, err := normalizeManagedGitFallbackRemoteURLs(req.FallbackRemoteURLs)
	if err != nil {
		status.setError("git_fallback_remotes_invalid", err.Error())
		return status
	}

	auxiliaryRefs, err := normalizeManagedGitAuxiliaryRefs(req.AuxiliaryRefs)
	if err != nil {
		status.setError("git_auxiliary_refs_invalid", err.Error())
		return status
	}

	credentialEnv, credentialCleanup, err := managedGitCredentialEnvironment(req.Credentials)
	if err != nil {
		status.setError("git_credentials_invalid", err.Error())
		return status
	}
	defer credentialCleanup()

	lock, err := acquireManagedGitWriterLock(ctx, checkoutPath)
	if err != nil {
		status.setError("git_lock_failed", err.Error())
		return status
	}

	defer func() {
		_ = lock.Close()
	}()

	checkoutStatus := NewManagedGitCheckout(checkoutPath).Status(ctx, "")
	if checkoutStatus.ErrorCode != "" {
		checkoutStatus.DefaultRef = ref
		return checkoutStatus
	}

	if err := configureManagedGitCheckout(ctx, checkoutPath, fallbackRemoteURLs); err != nil {
		checkoutStatus.DefaultRef = ref
		checkoutStatus.setError("git_config_failed", err.Error())
		return checkoutStatus
	}

	hydrationRemote, err := fetchManagedGitRef(ctx, checkoutPath, credentialEnv, ref, req.PreferredRemote)
	if err != nil {
		checkoutStatus.DefaultRef = ref
		if strings.TrimSpace(checkoutStatus.ErrorCode) == "" {
			if errors.Is(err, ErrNotFound) {
				checkoutStatus.setError("source_ref_not_found", err.Error())
			} else {
				checkoutStatus.setError("git_fetch_failed", err.Error())
			}
		}

		return checkoutStatus
	}

	if len(auxiliaryRefs) > 0 {
		hydratedAuxiliaryRefs, err := fetchManagedGitAuxiliaryRefs(ctx, checkoutPath, credentialEnv, auxiliaryRefs, hydrationRemote)
		if err != nil {
			checkoutStatus.DefaultRef = ref
			if strings.TrimSpace(checkoutStatus.ErrorCode) == "" {
				if errors.Is(err, ErrNotFound) {
					checkoutStatus.setError("source_auxiliary_ref_not_found", err.Error())
				} else {
					checkoutStatus.setError("git_auxiliary_fetch_failed", err.Error())
				}
			}

			return checkoutStatus
		}

		status.AuxiliaryRefs = hydratedAuxiliaryRefs
	}

	finalStatus := NewManagedGitCheckout(checkoutPath).Status(ctx, ref)
	finalStatus.HydrationRemote = hydrationRemote
	finalStatus.HydrationTier = managedGitHydrationTier(hydrationRemote)
	finalStatus.AuxiliaryRefs = status.AuxiliaryRefs
	return finalStatus
}

func NewManagedGitCheckout(checkoutPath string, opts ...GitCheckoutOption) *GitCheckout {
	return NewGitCheckout(checkoutPath, append([]GitCheckoutOption{WithRemoteFallback("origin"), withManagedWriterLock()}, opts...)...)
}

func managedGitCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	if creds.IsZero() {
		return nil, func() {}, nil
	}

	return gitCredentialEnvironment(creds)
}

func cloneManagedGitCheckout(ctx context.Context, checkoutPath, remoteURL string, env []string, fallbackRemoteURLs []string) error {
	remoteURL, err := normalizeGitRemoteURL(remoteURL)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(checkoutPath), 0o755); err != nil {
		return fmt.Errorf("create checkout parent: %w", err)
	}

	if _, err := runGitCommandWithEnv(ctx, env, managedGitCommandArgs("clone", "--filter=blob:none", "--no-checkout", "--no-tags", "--", remoteURL, checkoutPath)...); err != nil {
		return err
	}

	if err := configureManagedGitCheckout(ctx, checkoutPath, fallbackRemoteURLs); err != nil {
		return err
	}

	return nil
}

func fetchManagedGitCheckout(ctx context.Context, checkoutPath string, env []string, defaultRef string) error {
	if ref, ok := managedGitSyncFetchRef(ctx, checkoutPath, defaultRef); ok {
		if _, err := fetchManagedGitRef(ctx, checkoutPath, env, ref, ""); err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil
			}

			return err
		}

		return nil
	}

	_, err := (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, env, managedGitCommandArgs("fetch", "--filter=blob:none", "--prune", "--no-tags", "--no-auto-gc", "origin")...)
	return err
}

func managedGitSyncFetchRef(ctx context.Context, checkoutPath, defaultRef string) (string, bool) {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" || ref == "HEAD" {
		branch, ok := managedGitOriginHEADBranch(ctx, checkoutPath)
		if !ok {
			return "", false
		}

		return "refs/heads/" + branch, true
	}

	ref, err := normalizeRef(ref)
	if err != nil || looksLikeFullObjectID(ref) {
		return "", false
	}

	switch {
	case strings.HasPrefix(ref, "refs/heads/"), strings.HasPrefix(ref, "refs/tags/"), managedGitProviderRef(ref):
		return ref, true
	case strings.HasPrefix(ref, "refs/"):
		return "", false
	default:
		return ref, true
	}
}

func managedGitOriginHEADBranch(ctx context.Context, checkoutPath string) (string, bool) {
	var branch string
	if target, ok := managedGitSymbolicRefTarget(checkoutPath, "refs/remotes/origin/HEAD"); ok {
		branch = strings.TrimSpace(strings.TrimPrefix(target, "refs/remotes/origin/"))
	} else {
		out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
		if err != nil {
			return "", false
		}

		branch = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "origin/"))
	}

	if branch == "" || !validManagedBranchName(ctx, checkoutPath, branch) {
		return "", false
	}

	return branch, true
}

func fetchManagedGitMissingDefaultRef(ctx context.Context, checkoutPath, defaultRef string, env []string) error {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" || ref == "HEAD" || strings.HasPrefix(ref, "origin/") || looksLikeFullObjectID(ref) {
		return fmt.Errorf("%w: default ref is not a targeted fetch candidate", ErrInvalidReference)
	}

	normalized, err := normalizeRef(ref)
	if err != nil {
		return err
	}

	if strings.HasPrefix(normalized, "refs/heads/") ||
		strings.HasPrefix(normalized, "refs/tags/") ||
		managedGitProviderRef(normalized) {
		_, err := fetchManagedGitRef(ctx, checkoutPath, env, normalized, "")
		return err
	}

	if strings.HasPrefix(normalized, "refs/") {
		return fmt.Errorf("%w: default ref %q is not supported for targeted fallback", ErrInvalidReference, normalized)
	}

	tagRef, err := normalizeRef("refs/tags/" + normalized)
	if err != nil {
		return err
	}

	_, err = fetchManagedGitRef(ctx, checkoutPath, env, tagRef, "")
	return err
}

func fetchManagedGitRef(ctx context.Context, checkoutPath string, env []string, ref, preferredRemote string) (string, error) {
	ref, err := normalizeRef(ref)
	if err != nil {
		return "", err
	}

	sourceRef, destRef, ok := managedGitFetchRefspec(ref)
	if !ok {
		return "", fmt.Errorf("%w: managed fetch ref %q is not supported", ErrInvalidReference, ref)
	}

	candidateRef := managedGitCandidateRef()
	defer func() {
		_, _ = (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", "-d", candidateRef)
	}()

	refspec := "+" + sourceRef + ":" + candidateRef
	var fetchErrors []string
	var hydrationRemote string
	allFetchErrorsMissing := true
	for _, remote := range managedGitFetchRemotes(ctx, checkoutPath, preferredRemote) {
		if _, err := (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, env, managedGitCommandArgs("fetch", "--filter=blob:none", "--no-tags", "--no-auto-gc", remote, refspec)...); err != nil {
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", remote, err))
			if !managedGitFetchErrorIsMissingRef(err) {
				allFetchErrorsMissing = false
			}

			continue
		}

		fetchErrors = nil
		hydrationRemote = remote
		break
	}

	if len(fetchErrors) > 0 {
		err := fmt.Errorf("fetch managed ref %q failed from all candidate remotes: %s", ref, strings.Join(fetchErrors, "; "))
		if allFetchErrorsMissing {
			return "", fmt.Errorf("%w: %v", ErrNotFound, err)
		}

		return "", err
	}

	objectID, ok := managedGitResolveRef(checkoutPath, candidateRef)
	if !ok {
		objectOut, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "rev-parse", "--verify", candidateRef)
		if err != nil {
			return "", fmt.Errorf("%w: candidate ref %s did not resolve: %v", ErrNotFound, candidateRef, err)
		}

		objectID = strings.TrimSpace(string(objectOut))
	}

	if objectID == "" {
		return "", fmt.Errorf("%w: candidate ref %s resolved to an empty object", ErrInvalidReference, candidateRef)
	}

	commitOut, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "rev-parse", "--verify", candidateRef+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("%w: candidate ref %s did not resolve to a commit: %v", ErrNotFound, candidateRef, err)
	}

	if strings.TrimSpace(string(commitOut)) == "" {
		return "", fmt.Errorf("%w: candidate ref %s resolved to an empty commit", ErrInvalidReference, candidateRef)
	}

	if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", destRef, objectID); err != nil {
		return "", fmt.Errorf("publish managed ref %s: %w", destRef, err)
	}

	return hydrationRemote, nil
}

func fetchManagedGitAuxiliaryRefs(ctx context.Context, checkoutPath string, env []string, refs []string, preferredRemote string) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	hydratedRefs := make([]string, 0, len(refs))
	for _, ref := range refs {
		if _, err := fetchManagedGitAuxiliaryRef(ctx, checkoutPath, env, ref, preferredRemote); err != nil {
			return nil, err
		}

		hydratedRefs = append(hydratedRefs, ref)
	}

	return hydratedRefs, nil
}

func fetchManagedGitAuxiliaryRef(ctx context.Context, checkoutPath string, env []string, ref, preferredRemote string) (string, error) {
	sourceRef, destRef, ok := managedGitAuxiliaryFetchRefspec(ref)
	if !ok {
		return "", fmt.Errorf("%w: managed auxiliary ref %q is not supported", ErrInvalidReference, ref)
	}

	candidateRef := managedGitCandidateRef()
	defer func() {
		_, _ = (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", "-d", candidateRef)
	}()

	refspec := "+" + sourceRef + ":" + candidateRef
	var fetchErrors []string
	var hydrationRemote string
	allFetchErrorsMissing := true
	for _, remote := range managedGitFetchRemotes(ctx, checkoutPath, preferredRemote) {
		if _, err := (execGitRunner{}).RunGitWithInputEnv(ctx, checkoutPath, nil, env, managedGitCommandArgs("fetch", "--filter=blob:none", "--no-tags", "--no-auto-gc", remote, refspec)...); err != nil {
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", remote, err))
			if !managedGitFetchErrorIsMissingRef(err) {
				allFetchErrorsMissing = false
			}

			continue
		}

		fetchErrors = nil
		hydrationRemote = remote
		break
	}

	if len(fetchErrors) > 0 {
		err := fmt.Errorf("fetch managed auxiliary ref %q failed from all candidate remotes: %s", ref, strings.Join(fetchErrors, "; "))
		if allFetchErrorsMissing {
			return "", fmt.Errorf("%w: %v", ErrNotFound, err)
		}

		return "", err
	}

	commitOut, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "rev-parse", "--verify", candidateRef+"^{commit}")
	if err != nil {
		return "", fmt.Errorf("%w: auxiliary candidate ref %s did not resolve to a commit: %v", ErrNotFound, candidateRef, err)
	}

	commitID := strings.TrimSpace(string(commitOut))
	if commitID == "" {
		return "", fmt.Errorf("%w: auxiliary candidate ref %s resolved to an empty commit", ErrInvalidReference, candidateRef)
	}

	if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", destRef, commitID); err != nil {
		return "", fmt.Errorf("publish managed auxiliary ref %s: %w", destRef, err)
	}

	return hydrationRemote, nil
}

func managedGitCandidateRef() string {
	return "refs/vectis/candidates/" + uuid.NewString()
}

func managedGitFetchRemotes(ctx context.Context, checkoutPath, preferredRemote string) []string {
	remotes := []string{"origin"}
	remotes = append(remotes, managedGitFallbackRemoteNames(ctx, checkoutPath)...)
	return preferManagedGitFetchRemote(remotes, preferredRemote)
}

func managedGitFetchErrorIsMissingRef(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "couldn't find remote ref") ||
		strings.Contains(message, "could not find remote ref")
}

func preferManagedGitFetchRemote(remotes []string, preferredRemote string) []string {
	preferredRemote = strings.TrimSpace(preferredRemote)
	if preferredRemote == "" || len(remotes) < 2 {
		return remotes
	}

	var preferredIndex = -1
	for i, remote := range remotes {
		if remote == preferredRemote {
			preferredIndex = i
			break
		}
	}

	if preferredIndex <= 0 {
		return remotes
	}

	out := make([]string, 0, len(remotes))
	out = append(out, remotes[preferredIndex])
	out = append(out, remotes[:preferredIndex]...)
	out = append(out, remotes[preferredIndex+1:]...)
	return out
}

func managedGitFallbackRemoteNames(ctx context.Context, checkoutPath string) []string {
	if remotes, ok := managedGitFallbackRemoteConfig(checkoutPath); ok {
		return sortedManagedGitFallbackRemoteNames(remotes)
	}

	out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "remote")
	if err != nil {
		return nil
	}

	remotes := map[string]string{}
	for _, line := range strings.Split(string(out), "\n") {
		remote := strings.TrimSpace(line)
		if !validManagedGitFallbackRemote(remote) {
			continue
		}

		remotes[remote] = ""
	}

	return sortedManagedGitFallbackRemoteNames(remotes)
}

func managedGitFallbackRemoteConfig(checkoutPath string) (map[string]string, bool) {
	remoteURLs, err := gitcmd.WorkTreeRemoteURLs(checkoutPath)
	if err != nil {
		return nil, false
	}

	fallbacks := make(map[string]string, len(remoteURLs))
	for remote, remoteURL := range remoteURLs {
		if validManagedGitFallbackRemote(remote) {
			fallbacks[remote] = remoteURL
		}
	}

	return fallbacks, true
}

func sortedManagedGitFallbackRemoteNames(remotes map[string]string) []string {
	fallbacks := make([]managedGitFallbackRemote, 0, len(remotes))
	for remote := range remotes {
		tier, numeric := managedGitFallbackRemoteTier(remote)
		fallbacks = append(fallbacks, managedGitFallbackRemote{name: remote, tier: tier, numeric: numeric})
	}

	sort.Slice(fallbacks, func(i, j int) bool {
		if fallbacks[i].numeric != fallbacks[j].numeric {
			return fallbacks[i].numeric
		}

		if fallbacks[i].numeric && fallbacks[i].tier != fallbacks[j].tier {
			return fallbacks[i].tier < fallbacks[j].tier
		}

		return fallbacks[i].name < fallbacks[j].name
	})

	names := make([]string, 0, len(fallbacks))
	for _, remote := range fallbacks {
		names = append(names, remote.name)
	}

	return names
}

type managedGitFallbackRemote struct {
	name    string
	tier    int
	numeric bool
}

func managedGitFallbackRemoteTier(remote string) (int, bool) {
	suffix := strings.TrimPrefix(remote, "vectis-fallback-")
	tier, err := strconv.Atoi(suffix)
	if err != nil || tier < 0 {
		return 0, false
	}

	return tier, true
}

func validManagedGitFallbackRemote(remote string) bool {
	return strings.HasPrefix(remote, "vectis-fallback-") &&
		!strings.HasPrefix(remote, "-") &&
		!strings.ContainsAny(remote, "\x00\n\r") &&
		strings.TrimSpace(remote) == remote
}

func managedGitFetchRefspec(ref string) (string, string, bool) {
	ref = strings.TrimSpace(ref)
	switch {
	case ref == "" || ref == "HEAD" || looksLikeFullObjectID(ref):
		return "", "", false
	case strings.HasPrefix(ref, "refs/heads/"):
		branch := strings.TrimPrefix(ref, "refs/heads/")
		if branch == "" {
			return "", "", false
		}

		return ref, "refs/remotes/origin/" + branch, true
	case strings.HasPrefix(ref, "refs/tags/"):
		tag := strings.TrimPrefix(ref, "refs/tags/")
		if tag == "" {
			return "", "", false
		}

		return ref, ref, true
	case managedGitProviderRef(ref):
		return ref, managedGitHydratedRef(ref), true
	default:
		branch, ok := managedLocalBranchName(ref)
		if !ok || branch == "" {
			return "", "", false
		}

		return "refs/heads/" + branch, "refs/remotes/origin/" + branch, true
	}
}

func normalizeManagedGitAuxiliaryRefs(refs []string) ([]string, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(refs))
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		ref = strings.TrimSpace(ref)
		if ref == "" {
			return nil, fmt.Errorf("%w: auxiliary ref is required", ErrInvalidReference)
		}

		normalized, err := normalizeRef(ref)
		if err != nil {
			return nil, err
		}

		if !managedGitAuxiliaryRef(normalized) {
			return nil, fmt.Errorf("%w: auxiliary ref %q is not supported", ErrInvalidReference, normalized)
		}

		if _, ok := seen[normalized]; ok {
			continue
		}

		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	return out, nil
}

func managedGitAuxiliaryFetchRefspec(ref string) (string, string, bool) {
	ref = strings.TrimSpace(ref)
	if !managedGitAuxiliaryRef(ref) {
		return "", "", false
	}

	return ref, ref, true
}

func managedGitAuxiliaryRef(ref string) bool {
	ref = strings.TrimSpace(ref)
	return strings.HasPrefix(ref, "refs/notes/") && strings.TrimPrefix(ref, "refs/notes/") != ""
}

func managedGitProviderRef(ref string) bool {
	ref = strings.TrimSpace(ref)
	return strings.HasPrefix(ref, "refs/") &&
		!strings.HasPrefix(ref, "refs/heads/") &&
		!strings.HasPrefix(ref, "refs/tags/") &&
		!strings.HasPrefix(ref, "refs/remotes/") &&
		!strings.HasPrefix(ref, "refs/vectis/")
}

func managedGitHydratedRef(ref string) string {
	sum := sha256.Sum256([]byte(ref))
	return "refs/vectis/hydrated/" + hex.EncodeToString(sum[:])
}

func configureManagedGitCheckout(ctx context.Context, checkoutPath string, fallbackRemoteURLs []string) error {
	settings := gitcmd.NoAutoMaintenanceSettings()
	settings = append(settings, [2]string{"remote.origin.tagOpt", "--no-tags"})

	if err := gitcmd.WriteWorkTreeConfigSettings(checkoutPath, settings); err != nil {
		return fmt.Errorf("set managed checkout git config: %w", err)
	}

	if fallbackRemoteURLs != nil {
		if err := configureManagedGitFallbackRemotes(ctx, checkoutPath, fallbackRemoteURLs); err != nil {
			return err
		}
	}

	return nil
}

func configureManagedGitFallbackRemotes(ctx context.Context, checkoutPath string, fallbackRemoteURLs []string) error {
	desired := make(map[string]string, len(fallbackRemoteURLs))
	for i, remoteURL := range fallbackRemoteURLs {
		desired[managedGitFallbackRemoteName(i+1)] = remoteURL
	}

	current, configReadable := managedGitFallbackRemoteConfig(checkoutPath)
	currentNames := managedGitFallbackRemoteNames(ctx, checkoutPath)
	if configReadable {
		currentNames = sortedManagedGitFallbackRemoteNames(current)
	}

	for _, remote := range currentNames {
		if _, ok := desired[remote]; ok {
			continue
		}

		if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "remote", "remove", remote); err != nil {
			return fmt.Errorf("remove stale managed fallback remote %s: %w", remote, err)
		}
	}

	names := make([]string, 0, len(desired))
	for name := range desired {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		leftTier, _ := managedGitFallbackRemoteTier(names[i])
		rightTier, _ := managedGitFallbackRemoteTier(names[j])
		return leftTier < rightTier
	})

	for _, name := range names {
		if configReadable {
			if current[name] == desired[name] {
				continue
			}

			if err := writeManagedGitFallbackRemoteConfig(checkoutPath, name, desired[name]); err != nil {
				return fmt.Errorf("configure managed fallback remote %s: %w", name, err)
			}
			continue
		} else {
			_, _ = (execGitRunner{}).RunGit(ctx, checkoutPath, "remote", "remove", name)
		}

		if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "remote", "add", name, desired[name]); err != nil {
			return fmt.Errorf("configure managed fallback remote %s: %w", name, err)
		}
	}

	return nil
}

func writeManagedGitFallbackRemoteConfig(checkoutPath, name, remoteURL string) error {
	return gitcmd.WriteWorkTreeConfigSettings(checkoutPath, [][2]string{
		{"remote." + name + ".url", remoteURL},
		{"remote." + name + ".fetch", "+refs/heads/*:refs/remotes/" + name + "/*"},
	})
}

func managedGitFallbackRemoteName(tier int) string {
	return fmt.Sprintf("vectis-fallback-%d", tier)
}

func managedGitHydrationTier(remote string) string {
	remote = strings.TrimSpace(remote)
	switch {
	case remote == "":
		return ""
	case remote == "origin":
		return "origin"
	case strings.HasPrefix(remote, "vectis-fallback-"):
		return strings.TrimPrefix(remote, "vectis-")
	default:
		return "other"
	}
}

func managedGitCommandArgs(args ...string) []string {
	return gitcmd.NoAutoMaintenanceArgs(args...)
}

func alignManagedGitCheckoutRef(ctx context.Context, checkoutPath, defaultRef string) error {
	ref := strings.TrimSpace(defaultRef)
	if ref == "" {
		ref = "HEAD"
	}

	normalizedRef, ok := normalizeManagedGitSyncRef(ref)
	if !ok {
		return nil
	}
	ref = normalizedRef

	branch := ""
	if ref == "HEAD" {
		if target, ok := managedGitSymbolicRefTarget(checkoutPath, "refs/remotes/origin/HEAD"); ok {
			branch = strings.TrimSpace(strings.TrimPrefix(target, "refs/remotes/origin/"))
		} else {
			out, ok := managedGitCommandOutput(ctx, checkoutPath, "symbolic-ref", "--quiet", "--short", "refs/remotes/origin/HEAD")
			if !ok {
				return nil
			}

			branch = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(out)), "origin/"))
		}
	} else if b, ok := managedLocalBranchName(ref); ok {
		branch = b
	}

	if branch == "" || !validManagedBranchName(ctx, checkoutPath, branch) {
		return nil
	}

	remoteRef := "refs/remotes/origin/" + branch
	commit, ok := managedGitResolveRef(checkoutPath, remoteRef)
	if !ok {
		out, ok := managedGitCommandOutput(ctx, checkoutPath, "rev-parse", "--verify", remoteRef+"^{commit}")
		if !ok {
			return nil
		}

		commit = strings.TrimSpace(string(out))
	}
	if commit == "" {
		return fmt.Errorf("remote ref %s resolved to an empty commit", remoteRef)
	}

	if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "update-ref", "refs/heads/"+branch, commit); err != nil {
		return fmt.Errorf("update local branch %s: %w", branch, err)
	}

	if ref == "HEAD" {
		if _, err := (execGitRunner{}).RunGit(ctx, checkoutPath, "symbolic-ref", "HEAD", "refs/heads/"+branch); err != nil {
			return fmt.Errorf("set HEAD to %s: %w", branch, err)
		}
	}

	return nil
}

func normalizeManagedGitSyncRef(ref string) (string, bool) {
	normalized, err := normalizeRef(ref)
	return normalized, err == nil
}

func managedGitCommandOutput(ctx context.Context, checkoutPath string, args ...string) ([]byte, bool) {
	out, err := (execGitRunner{}).RunGit(ctx, checkoutPath, args...)
	return out, err == nil
}

func managedGitResolveRef(checkoutPath, ref string) (string, bool) {
	if strings.HasPrefix(ref, "refs/tags/") {
		return "", false
	}

	commit, ok, err := gitcmd.ResolveWorkTreeRef(checkoutPath, ref)
	if err != nil || !ok || !looksLikeFullObjectID(commit) {
		return "", false
	}

	return commit, true
}

func managedGitSymbolicRefTarget(checkoutPath, ref string) (string, bool) {
	target, ok, err := gitcmd.SymbolicWorkTreeRef(checkoutPath, ref)
	if err != nil || !ok {
		return "", false
	}

	return target, true
}

func managedLocalBranchName(ref string) (string, bool) {
	if strings.HasPrefix(ref, "refs/heads/") {
		branch := strings.TrimPrefix(ref, "refs/heads/")
		return branch, branch != ""
	}

	if strings.HasPrefix(ref, "refs/") || strings.HasPrefix(ref, "origin/") || looksLikeFullObjectID(ref) {
		return "", false
	}

	return ref, true
}

func remoteFallbackBranchName(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" || ref == "HEAD" || looksLikeFullObjectID(ref) {
		return "", false
	}

	if strings.HasPrefix(ref, "refs/heads/") {
		branch := strings.TrimPrefix(ref, "refs/heads/")
		return branch, branch != ""
	}

	if strings.HasPrefix(ref, "refs/") || strings.HasPrefix(ref, "origin/") {
		return "", false
	}

	return ref, true
}

func validManagedBranchName(ctx context.Context, checkoutPath, branch string) bool {
	_ = ctx
	_ = checkoutPath
	return validManagedBranchNameText(branch)
}

func validManagedBranchNameText(branch string) bool {
	if branch == "" || strings.TrimSpace(branch) != branch || strings.HasPrefix(branch, "-") {
		return false
	}

	if strings.HasPrefix(branch, "/") ||
		strings.HasSuffix(branch, "/") ||
		strings.HasSuffix(branch, ".") ||
		strings.Contains(branch, "//") ||
		strings.Contains(branch, "..") ||
		strings.Contains(branch, "@{") ||
		strings.ContainsAny(branch, "\x00\n\r\\ ~^:?*[") {
		return false
	}

	for _, part := range strings.Split(branch, "/") {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}

	return true
}

func NormalizeGitRemoteURL(remoteURL string) (string, error) {
	if strings.ContainsAny(remoteURL, "\x00\n\r") {
		return "", fmt.Errorf("%w: canonical_url is not a safe Git remote", ErrInvalidReference)
	}

	remoteURL = strings.TrimSpace(remoteURL)
	if remoteURL == "" {
		return "", fmt.Errorf("%w: canonical_url is required to clone a managed checkout", ErrInvalidReference)
	}

	if strings.HasPrefix(remoteURL, "-") {
		return "", fmt.Errorf("%w: canonical_url is not a safe Git remote", ErrInvalidReference)
	}

	return remoteURL, nil
}

func normalizeGitRemoteURL(remoteURL string) (string, error) {
	return NormalizeGitRemoteURL(remoteURL)
}

func normalizeManagedGitFallbackRemoteURLs(remoteURLs []string) ([]string, error) {
	if len(remoteURLs) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(remoteURLs))
	seen := make(map[string]struct{}, len(remoteURLs))
	for _, remoteURL := range remoteURLs {
		remoteURL, err := normalizeGitRemoteURL(remoteURL)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[remoteURL]; ok {
			continue
		}

		seen[remoteURL] = struct{}{}
		out = append(out, remoteURL)
	}

	if len(out) == 0 {
		return nil, nil
	}

	return out, nil
}

func directoryIsEmpty(dir string) bool {
	entries, err := os.ReadDir(dir)
	return err == nil && len(entries) == 0
}

func looksLikeFullObjectID(ref string) bool {
	if len(ref) != 40 && len(ref) != 64 {
		return false
	}

	for _, r := range ref {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}

	return true
}

func runGitCommand(ctx context.Context, args ...string) ([]byte, error) {
	return runGitCommandWithEnv(ctx, nil, args...)
}

func runGitCommandWithEnv(ctx context.Context, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = gitCommandEnv(env)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return nil, fmt.Errorf("%w: %s", err, msg)
		}

		return nil, err
	}

	return out, nil
}
