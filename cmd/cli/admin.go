package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/spf13/cobra"
)

type namespaceCLIResponse struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	ParentID         *int64 `json:"parent_id,omitempty"`
	Path             string `json:"path"`
	BreakInheritance bool   `json:"break_inheritance"`
}

type userCLIResponse struct {
	ID              int64  `json:"id"`
	Username        string `json:"username"`
	Enabled         bool   `json:"enabled"`
	CreatedAt       string `json:"created_at"`
	InitialPassword string `json:"initial_password,omitempty"`
}

type bindingCLIResponse struct {
	LocalUserID int64  `json:"local_user_id"`
	Username    string `json:"username,omitempty"`
	Role        string `json:"role"`
}

func apiStatusError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var apiErr struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   string `json:"error"`
		Detail  string `json:"detail"`
	}

	if json.Unmarshal(body, &apiErr) == nil {
		if apiErr.Code != "" {
			if apiErr.Message != "" {
				return fmt.Errorf("%s: %s", apiErr.Code, apiErr.Message)
			}

			return fmt.Errorf("%s", apiErr.Code)
		}

		if apiErr.Error != "" {
			if apiErr.Detail != "" {
				return fmt.Errorf("%s: %s", apiErr.Error, apiErr.Detail)
			}

			return fmt.Errorf("%s", apiErr.Error)
		}
	}

	return fmt.Errorf("unexpected status: %s", resp.Status)
}

func jsonAPIRequest(method, path string, payload any) (*http.Request, error) {
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("encode request: %w", err)
		}

		body = bytes.NewReader(b)
	}

	req, err := newAPIRequest(method, path, body)
	if err != nil {
		return nil, err
	}

	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

func doJSONAPI(method, path string, payload any, wantStatus int, out any) error {
	req, err := jsonAPIRequest(method, path, payload)
	if err != nil {
		return err
	}

	resp, err := doAPIRequest(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != wantStatus {
		return apiStatusError(resp)
	}

	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("parse response: %w", err)
		}
	}

	return nil
}

func parseInt64Arg(name, raw string) (int64, error) {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}

	return n, nil
}

func namespaceList(w io.Writer) error {
	var namespaces []namespaceCLIResponse
	if err := doJSONAPI(http.MethodGet, "/api/v1/namespaces", nil, http.StatusOK, &namespaces); err != nil {
		return err
	}

	for _, ns := range namespaces {
		parent := "-"
		if ns.ParentID != nil {
			parent = strconv.FormatInt(*ns.ParentID, 10)
		}

		fmt.Fprintf(w, "%d\t%s\tname=%s\tparent=%s\tbreak_inheritance=%t\n", ns.ID, ns.Path, ns.Name, parent, ns.BreakInheritance)
	}

	return nil
}

func namespaceGet(id int64, w io.Writer) error {
	var ns namespaceCLIResponse
	if err := doJSONAPI(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%d", id), nil, http.StatusOK, &ns); err != nil {
		return err
	}

	parent := "-"
	if ns.ParentID != nil {
		parent = strconv.FormatInt(*ns.ParentID, 10)
	}

	fmt.Fprintf(w, "id=%d\nname=%s\npath=%s\nparent_id=%s\nbreak_inheritance=%t\n", ns.ID, ns.Name, ns.Path, parent, ns.BreakInheritance)
	return nil
}

func namespaceCreate(name string, parentID int64, w io.Writer) error {
	payload := map[string]any{"name": name}
	if parentID > 0 {
		payload["parent_id"] = parentID
	}

	var ns namespaceCLIResponse
	if err := doJSONAPI(http.MethodPost, "/api/v1/namespaces", payload, http.StatusCreated, &ns); err != nil {
		return err
	}

	fmt.Fprintf(w, "Namespace created: %d %s\n", ns.ID, ns.Path)
	return nil
}

func namespaceDelete(id int64) error {
	return doJSONAPI(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d", id), nil, http.StatusNoContent, nil)
}

func userList(w io.Writer) error {
	var users []userCLIResponse
	if err := doJSONAPI(http.MethodGet, "/api/v1/users", nil, http.StatusOK, &users); err != nil {
		return err
	}

	for _, u := range users {
		fmt.Fprintf(w, "%d\t%s\tenabled=%t\tcreated=%s\n", u.ID, u.Username, u.Enabled, u.CreatedAt)
	}

	return nil
}

func userGet(id int64, w io.Writer) error {
	var u userCLIResponse
	if err := doJSONAPI(http.MethodGet, fmt.Sprintf("/api/v1/users/%d", id), nil, http.StatusOK, &u); err != nil {
		return err
	}

	fmt.Fprintf(w, "id=%d\nusername=%s\nenabled=%t\ncreated_at=%s\n", u.ID, u.Username, u.Enabled, u.CreatedAt)
	return nil
}

func userCreate(username, password string, w io.Writer) error {
	payload := map[string]any{"username": username}
	if password != "" {
		payload["password"] = password
	}

	var u userCLIResponse
	if err := doJSONAPI(http.MethodPost, "/api/v1/users", payload, http.StatusCreated, &u); err != nil {
		return err
	}

	fmt.Fprintf(w, "User created: %d %s\n", u.ID, u.Username)
	if u.InitialPassword != "" {
		fmt.Fprintf(w, "Initial password: %s\n", u.InitialPassword)
	}

	return nil
}

func userSetEnabled(id int64, enabled bool) error {
	return doJSONAPI(http.MethodPut, fmt.Sprintf("/api/v1/users/%d", id), map[string]any{"enabled": enabled}, http.StatusNoContent, nil)
}

func userDelete(id int64) error {
	return doJSONAPI(http.MethodDelete, fmt.Sprintf("/api/v1/users/%d", id), nil, http.StatusNoContent, nil)
}

func userChangePassword(userID int64, currentPassword, newPassword string) error {
	payload := map[string]any{"new_password": newPassword}
	if currentPassword != "" {
		payload["current_password"] = currentPassword
	}

	if userID > 0 {
		payload["user_id"] = userID
	}

	return doJSONAPI(http.MethodPost, "/api/v1/users/change-password", payload, http.StatusNoContent, nil)
}

func bindingList(namespaceID int64, w io.Writer) error {
	var bindings []bindingCLIResponse
	if err := doJSONAPI(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%d/bindings", namespaceID), nil, http.StatusOK, &bindings); err != nil {
		return err
	}

	for _, b := range bindings {
		user := strconv.FormatInt(b.LocalUserID, 10)
		if b.Username != "" {
			user += "/" + b.Username
		}

		fmt.Fprintf(w, "%s\trole=%s\n", user, b.Role)
	}

	return nil
}

func bindingCreate(namespaceID, userID int64, role string, w io.Writer) error {
	var binding bindingCLIResponse
	if err := doJSONAPI(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%d/bindings", namespaceID), map[string]any{
		"local_user_id": userID,
		"role":          role,
	}, http.StatusCreated, &binding); err != nil {
		return err
	}

	fmt.Fprintf(w, "Role binding created: namespace=%d user=%d role=%s\n", namespaceID, binding.LocalUserID, binding.Role)
	return nil
}

func bindingDelete(namespaceID, userID int64, role string) error {
	path := fmt.Sprintf("/api/v1/namespaces/%d/bindings/%d?role=%s", namespaceID, userID, url.QueryEscape(role))
	return doJSONAPI(http.MethodDelete, path, nil, http.StatusNoContent, nil)
}

func runNamespaceList(cmd *cobra.Command, args []string) {
	runCLIError(namespaceList(os.Stdout))
}

func runNamespaceGet(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("namespace id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(namespaceGet(id, os.Stdout))
}

func runNamespaceCreate(cmd *cobra.Command, args []string) {
	parentID, _ := cmd.Flags().GetInt64("parent-id")
	runCLIError(namespaceCreate(args[0], parentID, os.Stdout))
}

func runNamespaceDelete(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("namespace id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(namespaceDelete(id))
	fmt.Println("Namespace deleted.")
}

func runUserList(cmd *cobra.Command, args []string) {
	runCLIError(userList(os.Stdout))
}

func runUserGet(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("user id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(userGet(id, os.Stdout))
}

func runUserCreate(cmd *cobra.Command, args []string) {
	password, _ := cmd.Flags().GetString("password")
	runCLIError(userCreate(args[0], password, os.Stdout))
}

func runUserUpdate(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("user id", args[0])
	if err != nil {
		runCLIError(err)
	}

	enabled, _ := cmd.Flags().GetBool("enabled")
	runCLIError(userSetEnabled(id, enabled))
	fmt.Println("User updated.")
}

func runUserEnable(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("user id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(userSetEnabled(id, true))
	fmt.Println("User enabled.")
}

func runUserDisable(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("user id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(userSetEnabled(id, false))
	fmt.Println("User disabled.")
}

func runUserDelete(cmd *cobra.Command, args []string) {
	id, err := parseInt64Arg("user id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(userDelete(id))
	fmt.Println("User deleted.")
}

func runUserChangePassword(cmd *cobra.Command, args []string) {
	userID, _ := cmd.Flags().GetInt64("user-id")
	currentPassword, _ := cmd.Flags().GetString("current-password")
	newPassword, _ := cmd.Flags().GetString("new-password")
	if newPassword == "" {
		runCLIError(fmt.Errorf("--new-password is required"))
	}

	runCLIError(userChangePassword(userID, currentPassword, newPassword))
	fmt.Println("Password changed.")
}

func runBindingList(cmd *cobra.Command, args []string) {
	nsID, err := parseInt64Arg("namespace id", args[0])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(bindingList(nsID, os.Stdout))
}

func runBindingCreate(cmd *cobra.Command, args []string) {
	nsID, err := parseInt64Arg("namespace id", args[0])
	if err != nil {
		runCLIError(err)
	}

	userID, err := parseInt64Arg("user id", args[1])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(bindingCreate(nsID, userID, args[2], os.Stdout))
}

func runBindingDelete(cmd *cobra.Command, args []string) {
	nsID, err := parseInt64Arg("namespace id", args[0])
	if err != nil {
		runCLIError(err)
	}

	userID, err := parseInt64Arg("user id", args[1])
	if err != nil {
		runCLIError(err)
	}

	runCLIError(bindingDelete(nsID, userID, args[2]))
	fmt.Println("Role binding deleted.")
}

func runCLIError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var namespaceCmd = &cobra.Command{
	Use:   "namespaces",
	Short: "List, show, create, and delete namespaces",
}

var namespaceListCmd = &cobra.Command{Use: "list", Short: "List namespaces", Run: runNamespaceList}
var namespaceGetCmd = &cobra.Command{Use: "show [namespace-id]", Short: "Show a namespace", Args: cobra.ExactArgs(1), Run: runNamespaceGet}
var namespaceCreateCmd = &cobra.Command{Use: "create [name]", Short: "Create a namespace", Args: cobra.ExactArgs(1), Run: runNamespaceCreate}
var namespaceDeleteCmd = &cobra.Command{Use: "delete [namespace-id]", Short: "Delete an empty namespace", Args: cobra.ExactArgs(1), Run: runNamespaceDelete}

var userCmd = &cobra.Command{
	Use:   "users",
	Short: "List, show, create, and manage local users",
}

var userListCmd = &cobra.Command{Use: "list", Short: "List users", Run: runUserList}
var userGetCmd = &cobra.Command{Use: "show [user-id]", Short: "Show a user", Args: cobra.ExactArgs(1), Run: runUserGet}
var userCreateCmd = &cobra.Command{Use: "create [username]", Short: "Create a user", Args: cobra.ExactArgs(1), Run: runUserCreate}
var userEnableCmd = &cobra.Command{Use: "enable [user-id]", Short: "Enable a user", Args: cobra.ExactArgs(1), Run: runUserEnable}
var userDisableCmd = &cobra.Command{Use: "disable [user-id]", Short: "Disable a user", Args: cobra.ExactArgs(1), Run: runUserDisable}
var userDeleteCmd = &cobra.Command{Use: "delete [user-id]", Short: "Delete a user", Args: cobra.ExactArgs(1), Run: runUserDelete}
var userChangePasswordCmd = &cobra.Command{Use: "change-password", Short: "Change a user password", Run: runUserChangePassword}

var roleBindingCmd = &cobra.Command{
	Use:   "role-bindings",
	Short: "List, grant, and revoke namespace role bindings",
}

var roleBindingListCmd = &cobra.Command{Use: "list [namespace-id]", Short: "List role bindings", Args: cobra.ExactArgs(1), Run: runBindingList}
var roleBindingCreateCmd = &cobra.Command{Use: "grant [namespace-id] [user-id] [role]", Short: "Grant a namespace role", Args: cobra.ExactArgs(3), Run: runBindingCreate}
var roleBindingDeleteCmd = &cobra.Command{Use: "revoke [namespace-id] [user-id] [role]", Short: "Revoke a namespace role", Args: cobra.ExactArgs(3), Run: runBindingDelete}
