package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	encryptedfs "vectis/extensions/secrets/encryptedfs"
)

type secretEncryptedFSPutResult struct {
	Status     string `json:"status"`
	Ref        string `json:"ref"`
	Path       string `json:"path"`
	KeyFile    string `json:"key_file"`
	CreatedKey bool   `json:"created_key,omitempty"`
	Bytes      int    `json:"bytes"`
}

var (
	secretEncryptedFSRoot      string
	secretEncryptedFSKeyFile   string
	secretEncryptedFSFromFile  string
	secretEncryptedFSCreateKey bool
	secretEncryptedFSForce     bool
)

func runSecretEncryptedFSPut(cmd *cobra.Command, args []string) {
	runCLIError(secretEncryptedFSPut(
		args[0],
		secretEncryptedFSRoot,
		secretEncryptedFSKeyFile,
		secretEncryptedFSFromFile,
		secretEncryptedFSCreateKey,
		secretEncryptedFSForce,
		os.Stdin,
		os.Stdout,
	))
}

func secretEncryptedFSPut(ref, root, keyFile, fromFile string, createKey, force bool, stdin io.Reader, out io.Writer) error {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return fmt.Errorf("encryptedfs ref is required")
	}

	target, err := encryptedfs.EncryptedFSSecretFilePath(root, ref)
	if err != nil {
		return err
	}

	plaintext, err := readSecretPlaintext(fromFile, stdin, encryptedfs.DefaultMaxSecretBytes)
	if err != nil {
		return err
	}

	key, createdKey, err := loadEncryptedFSWriteKey(keyFile, createKey)
	if err != nil {
		return err
	}

	if force {
		err = encryptedfs.WriteEncryptedFSSecretFile(root, ref, plaintext, key)
	} else {
		err = encryptedfs.WriteEncryptedFSSecretFileExclusive(root, ref, plaintext, key)
	}

	if err != nil {
		return err
	}

	result := secretEncryptedFSPutResult{
		Status:     "ok",
		Ref:        ref,
		Path:       target,
		KeyFile:    strings.TrimSpace(keyFile),
		CreatedKey: createdKey,
		Bytes:      len(plaintext),
	}

	if outputIsJSON() {
		return writeJSON(out, result)
	}

	if _, err := fmt.Fprintf(out, "Encrypted secret written: %s\n", target); err != nil {
		return err
	}

	if createdKey {
		_, err = fmt.Fprintf(out, "Created encryptedfs key file: %s\n", result.KeyFile)
	}

	return err
}

func loadEncryptedFSWriteKey(path string, create bool) ([]byte, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, false, fmt.Errorf("--key-file is required")
	}

	if !create {
		key, err := encryptedfs.LoadEncryptedFSKeyFile(path)
		return key, false, err
	}

	created := false
	if _, err := os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, false, fmt.Errorf("stat encryptedfs key file: %w", err)
		}

		created = true
	}

	key, err := encryptedfs.EnsureEncryptedFSKeyFile(path)
	if err != nil {
		return nil, false, err
	}

	return key, created, nil
}

func readSecretPlaintext(source string, stdin io.Reader, limit int64) ([]byte, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		source = "-"
	}

	var (
		reader io.Reader
		file   *os.File
		err    error
	)

	if source == "-" {
		reader = stdin
	} else {
		file, err = os.Open(source)
		if err != nil {
			return nil, fmt.Errorf("read secret source: %w", err)
		}
		defer closeIgnoringError(file)

		reader = file
	}

	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read secret source: %w", err)
	}

	if int64(len(data)) > limit {
		return nil, fmt.Errorf("secret source exceeds %d bytes", limit)
	}

	return data, nil
}

var secretsCmd = &cobra.Command{
	Use:   "secrets",
	Short: "Manage job secret stores",
	Run:   showCommandHelp,
}

var secretsEncryptedFSCmd = &cobra.Command{
	Use:   "encryptedfs",
	Short: "Manage encrypted filesystem-backed job secrets",
	Run:   showCommandHelp,
}

var secretsEncryptedFSPutCmd = &cobra.Command{
	Use:   "put [ref]",
	Short: "Encrypt and write an encryptedfs job secret",
	Long:  `Encrypt a secret value into an encryptedfs envelope file for the cell-local vectis-secrets broker. Secret values are read from stdin by default.`,
	Args:  cobra.ExactArgs(1),
	Run:   runSecretEncryptedFSPut,
}

func configureSecretEncryptedFSPutFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&secretEncryptedFSRoot, "root", "", "Encryptedfs provider root directory")
	cmd.Flags().StringVar(&secretEncryptedFSKeyFile, "key-file", "", "Encryptedfs provider key file")
	cmd.Flags().StringVar(&secretEncryptedFSFromFile, "from-file", "-", "Read secret plaintext from this file, or '-' for stdin")
	cmd.Flags().BoolVar(&secretEncryptedFSCreateKey, "create-key", false, "Create the encryptedfs key file if it does not exist")
	cmd.Flags().BoolVar(&secretEncryptedFSForce, "force", false, "Overwrite an existing encryptedfs secret envelope")
}
