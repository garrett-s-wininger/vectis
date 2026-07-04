package api

import (
	"net/http"
	"time"
)

const (
	sessionCookieName = "__Host-vectis_session"
	csrfCookieName    = "__Host-vectis_csrf"
	csrfHeaderName    = "X-CSRF-Token"

	logoutClearSiteData = `"cache", "storage"`
)

func setSessionCookies(w http.ResponseWriter, _ *http.Request, sessionToken, csrfToken string, expiresAt time.Time) {
	maxAge := max(int(time.Until(expiresAt).Seconds()), 0)

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionToken,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    csrfToken,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: false,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookies(w http.ResponseWriter, _ *http.Request) {
	clearCookie(w, sessionCookieName)
	clearCookie(w, csrfCookieName)
}

func clearLogoutSiteData(w http.ResponseWriter) {
	w.Header().Set("Clear-Site-Data", logoutClearSiteData)
}

func clearCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0).UTC(),
		MaxAge:   -1,
		HttpOnly: name == sessionCookieName,
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
}
