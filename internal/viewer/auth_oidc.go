package viewer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

var (
	oidcMu     sync.Mutex
	oidcStates = map[string]*oidcPending{}
)

type oidcPending struct {
	Verifier string
	Nonce    string
}

func (s *Server) handleOIDCAuthorize(w http.ResponseWriter, r *http.Request) {
	oauth2Cfg, provider, err := s.oidcConfig(r.Context())
	if err != nil {
		http.Error(w, "OIDC not configured: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	_ = provider

	stateBytes := make([]byte, 16)
	rand.Read(stateBytes)
	state := hex.EncodeToString(stateBytes)

	nonceBytes := make([]byte, 16)
	rand.Read(nonceBytes)
	nonce := hex.EncodeToString(nonceBytes)

	verifier := oauth2.GenerateVerifier()

	oidcMu.Lock()
	oidcStates[state] = &oidcPending{Verifier: verifier, Nonce: nonce}
	oidcMu.Unlock()

	url := oauth2Cfg.AuthCodeURL(state,
		oauth2.AccessTypeOffline,
		oidc.Nonce(nonce),
		oauth2.S256ChallengeOption(verifier),
	)

	http.Redirect(w, r, url, http.StatusFound)
}

func (s *Server) handleOIDCCallback(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	if state == "" || code == "" {
		http.Error(w, "missing state or code", http.StatusBadRequest)
		return
	}

	oidcMu.Lock()
	pending, ok := oidcStates[state]
	delete(oidcStates, state)
	oidcMu.Unlock()
	if !ok {
		http.Error(w, "invalid state", http.StatusBadRequest)
		return
	}

	oauth2Cfg, provider, err := s.oidcConfig(ctx)
	if err != nil {
		http.Error(w, "OIDC config error", http.StatusInternalServerError)
		return
	}

	token, err := oauth2Cfg.Exchange(ctx, code, oauth2.VerifierOption(pending.Verifier))
	if err != nil {
		slog.Error("OIDC token exchange failed", "error", err)
		http.Error(w, "token exchange failed", http.StatusUnauthorized)
		return
	}

	verifier := provider.Verifier(&oidc.Config{ClientID: oauth2Cfg.ClientID})
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok {
		http.Error(w, "no id_token in response", http.StatusUnauthorized)
		return
	}

	idToken, err := verifier.Verify(ctx, rawIDToken)
	if err != nil {
		slog.Error("OIDC token verification failed", "error", err)
		http.Error(w, "token verification failed", http.StatusUnauthorized)
		return
	}

	if idToken.Nonce != pending.Nonce {
		http.Error(w, "nonce mismatch", http.StatusUnauthorized)
		return
	}

	var claims struct {
		Sub               string `json:"sub"`
		PreferredUsername  string `json:"preferred_username"`
		Name              string `json:"name"`
		Email             string `json:"email"`
		Picture           string `json:"picture"`
	}
	if err := idToken.Claims(&claims); err != nil {
		http.Error(w, "failed to parse claims", http.StatusInternalServerError)
		return
	}

	// Also try userinfo for more complete data
	userInfo, err := provider.UserInfo(ctx, oauth2.StaticTokenSource(token))
	if err == nil {
		var uiClaims struct {
			Sub              string `json:"sub"`
			PreferredUsername string `json:"preferred_username"`
			Name             string `json:"name"`
			Email            string `json:"email"`
			Picture          string `json:"picture"`
		}
		if userInfo.Claims(&uiClaims) == nil {
			if claims.Email == "" {
				claims.Email = uiClaims.Email
			}
			if claims.Name == "" {
				claims.Name = uiClaims.Name
			}
			if claims.PreferredUsername == "" {
				claims.PreferredUsername = uiClaims.PreferredUsername
			}
			if claims.Picture == "" {
				claims.Picture = uiClaims.Picture
			}
		}
	}

	username := claims.PreferredUsername
	if username == "" {
		username = claims.Email
	}
	if username == "" {
		username = claims.Sub
	}

	displayName := claims.Name
	if displayName == "" {
		displayName = username
	}

	ident := ExternalIdentity{
		Provider:    "oidc",
		ProviderID:  claims.Sub,
		Username:    username,
		DisplayName: displayName,
		Email:       claims.Email,
		AvatarURL:   claims.Picture,
	}

	viewerUser, err := s.findOrCreateViewerUser(ident)
	if err != nil {
		http.Error(w, "failed to create user", http.StatusInternalServerError)
		return
	}

	if err := s.createSession(w, r, viewerUser.ID); err != nil {
		http.Error(w, "failed to create session", http.StatusInternalServerError)
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) oidcConfig(ctx context.Context) (*oauth2.Config, *oidc.Provider, error) {
	issuer := s.Config.GetString("viewer_oidc_issuer_url")
	clientID := s.Config.GetString("viewer_oidc_client_id")
	clientSecret := s.Config.GetString("viewer_oidc_client_secret")
	redirectURI := s.Config.GetString("viewer_oidc_redirect_uri")

	if issuer == "" || clientID == "" {
		return nil, nil, fmt.Errorf("OIDC issuer and client ID required")
	}

	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, nil, fmt.Errorf("OIDC discovery: %w", err)
	}

	scopes := strings.Fields(s.Config.GetString("viewer_oidc_scopes"))
	if len(scopes) == 0 {
		scopes = []string{oidc.ScopeOpenID, "profile", "email"}
	}

	cfg := &oauth2.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		RedirectURL:  redirectURI,
		Endpoint:     provider.Endpoint(),
		Scopes:       scopes,
	}

	return cfg, provider, nil
}
