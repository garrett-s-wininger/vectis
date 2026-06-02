package api

import (
	"net/http"
	"time"

	"vectis/internal/config"
)

const (
	sessionCookieName = "vectis_session"
	csrfCookieName    = "vectis_csrf"
	csrfHeaderName    = "X-CSRF-Token"
)

func sessionCookieSecure(r *http.Request) bool {
	return config.APISessionCookieSecure() || requestUsesTLS(r)
}

func requestUsesTLS(r *http.Request) bool {
	return r != nil && r.TLS != nil
}

func setSessionCookies(w http.ResponseWriter, r *http.Request, sessionToken, csrfToken string, expiresAt time.Time) {
	maxAge := max(int(time.Until(expiresAt).Seconds()), 0)

	secure := sessionCookieSecure(r)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionToken,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})

	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    csrfToken,
		Path:     "/",
		Expires:  expiresAt,
		MaxAge:   maxAge,
		HttpOnly: false,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookies(w http.ResponseWriter, r *http.Request) {
	secure := sessionCookieSecure(r)
	for _, name := range []string{sessionCookieName, csrfCookieName} {
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			Expires:  time.Unix(0, 0).UTC(),
			MaxAge:   -1,
			HttpOnly: name == sessionCookieName,
			Secure:   secure,
			SameSite: http.SameSiteLaxMode,
		})
	}
}
