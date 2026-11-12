package main

// Controller login and authorization (docs/collins_design.md, section 5).
//
// Accounts live in a JSON file (USERS_FILE, mounted read-only into the
// gateway). Each has a role, the sectors it controls, and a bcrypt hash of
// its password; the password itself is never stored.
//
// POST /api/login checks a username and password and returns a JWT signed
// with HS256 and JWT_SECRET. The token carries who you are (sub), your role
// and your sectors, and expires after 30 minutes. Anyone can read a token,
// but nobody without the secret can change it without breaking the
// signature, so the gateway trusts its claims without a session table.
//
// POST /api/clearance needs "Authorization: Bearer <token>":
//
//	no token, bad signature, expired      -> 401
//	controller, aircraft not in sectors   -> 403
//	supervisor                            -> any sector
//
// The sector check runs inside tryClearance, after the owner has been read
// from the registry leader, so a buffered clearance is checked again when it
// is flushed. Watching the radar (/api/state, /ws) stays open.
//
// Simplifications for the course project: one shared HS256 secret (no key
// rotation), no refresh tokens or logout list (a stolen token works until it
// expires), no rate limiting of login attempts.

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// Roles.
const (
	RoleController = "controller" // clearances for aircraft in its own sectors
	RoleSupervisor = "supervisor" // clearances for any sector
)

const tokenTTL = 30 * time.Minute

type user struct {
	Username string   `json:"username"`
	Bcrypt   string   `json:"bcrypt"`
	Role     string   `json:"role"`
	Sectors  []string `json:"sectors"`
}

// claims is what the gateway signs into a token.
type claims struct {
	Role    string   `json:"role"`
	Sectors []string `json:"sectors"`
	jwt.RegisteredClaims
}

// scope is who issued a clearance and what they may clear. A clearance with
// a nil scope was sent while login was off, and is not checked.
type scope struct {
	role    string
	sectors []string
}

// allows reports whether this scope may issue clearances in sector.
func (s *scope) allows(sector string) bool {
	return s == nil || s.role == RoleSupervisor || slices.Contains(s.sectors, sector)
}

type securityStats struct {
	Logins       int `json:"logins"`
	FailedLogins int `json:"failed_logins"`
	Unauthorized int `json:"unauthorized"` // clearance without a valid token (401)
	Forbidden    int `json:"forbidden"`    // clearance outside the controller's sectors (403)
}

var errBadCredentials = errors.New("invalid username or password")

type authenticator struct {
	users  map[string]user
	secret []byte
	ttl    time.Duration
	now    func() time.Time
	// dummy is checked when the username does not exist, so a wrong username
	// takes as long as a wrong password and does not reveal which accounts exist.
	dummy []byte
}

func newAuthenticator(users []user, secret []byte) (*authenticator, error) {
	if len(secret) < 32 {
		return nil, errors.New("JWT secret must be at least 32 bytes")
	}
	a := &authenticator{users: map[string]user{}, secret: secret, ttl: tokenTTL, now: time.Now}
	for _, u := range users {
		if err := validUser(u); err != nil {
			return nil, err
		}
		if _, dup := a.users[u.Username]; dup {
			return nil, fmt.Errorf("duplicate user %q", u.Username)
		}
		a.users[u.Username] = u
	}
	if len(a.users) == 0 {
		return nil, errors.New("no users")
	}
	// Same cost as the real hashes, so both paths take the same time.
	cost := bcrypt.MinCost
	for _, u := range a.users {
		c, _ := bcrypt.Cost([]byte(u.Bcrypt))
		cost = max(cost, c)
	}
	a.dummy, _ = bcrypt.GenerateFromPassword([]byte("no such user"), cost)
	return a, nil
}

func validUser(u user) error {
	switch {
	case strings.TrimSpace(u.Username) == "" || u.Username != strings.TrimSpace(u.Username):
		return fmt.Errorf("user %q: bad username", u.Username)
	case u.Role != RoleController && u.Role != RoleSupervisor:
		return fmt.Errorf("user %q: role must be %q or %q", u.Username, RoleController, RoleSupervisor)
	case u.Role == RoleController && len(u.Sectors) == 0:
		return fmt.Errorf("user %q: a controller needs at least one sector", u.Username)
	}
	if _, err := bcrypt.Cost([]byte(u.Bcrypt)); err != nil {
		return fmt.Errorf("user %q: bcrypt field is not a bcrypt hash (make one with cmd/hashpw)", u.Username)
	}
	return nil
}

// authFromEnv loads USERS_FILE and JWT_SECRET. Login can only be turned off
// on purpose (AUTH=off), never by a missing file, so a typo in the compose
// file cannot silently open clearances to everyone.
func authFromEnv() *authenticator {
	if os.Getenv("AUTH") == "off" {
		log.Printf("auth OFF: anyone can issue clearances")
		return nil
	}
	path := envOr("USERS_FILE", "/etc/atc/users.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("auth: %v (set USERS_FILE, or AUTH=off to run without login)", err)
	}
	var users []user
	if err := json.Unmarshal(raw, &users); err != nil {
		log.Fatalf("auth: %s: %v", path, err)
	}
	secret := []byte(os.Getenv("JWT_SECRET"))
	if len(secret) == 0 {
		// Fine for a demo: tokens just stop working when the gateway restarts.
		secret = make([]byte, 32)
		rand.Read(secret)
		log.Printf("auth: JWT_SECRET not set, using a random secret for this run")
	}
	a, err := newAuthenticator(users, secret)
	if err != nil {
		log.Fatalf("auth: %s: %v", path, err)
	}
	log.Printf("auth on: %d users from %s", len(a.users), path)
	return a
}

// login checks a password and returns a signed token.
func (a *authenticator) login(username, password string) (string, *claims, error) {
	u, ok := a.users[username]
	if !ok {
		bcrypt.CompareHashAndPassword(a.dummy, []byte(password)) // same cost as a real check
		return "", nil, errBadCredentials
	}
	if bcrypt.CompareHashAndPassword([]byte(u.Bcrypt), []byte(password)) != nil {
		return "", nil, errBadCredentials
	}
	now := a.now()
	c := &claims{
		Role:    u.Role,
		Sectors: u.Sectors,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   u.Username,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(a.ttl)),
		},
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(a.secret)
	return tok, c, err
}

// verify checks a token's signature, algorithm and expiry, and returns its claims.
func (a *authenticator) verify(token string) (*claims, error) {
	if token == "" {
		return nil, errors.New("no Authorization: Bearer token")
	}
	c := &claims{}
	_, err := jwt.ParseWithClaims(token, c,
		func(*jwt.Token) (any, error) { return a.secret, nil },
		// Only accept HS256. Without this, a token claiming "alg": "none" or
		// another algorithm could be tried against the key.
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(a.now),
	)
	if err != nil {
		return nil, err
	}
	// A token is only as good as the account behind it: refuse tokens for
	// users that were removed from the file since the token was issued.
	if _, ok := a.users[c.Subject]; !ok {
		return nil, errors.New("unknown user")
	}
	if c.Role != RoleController && c.Role != RoleSupervisor {
		return nil, errors.New("bad role")
	}
	return c, nil
}

// bearer returns the token from "Authorization: Bearer <token>", or "".
func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// ---------- HTTP ----------

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token       string   `json:"token"`
	Username    string   `json:"username"`
	Role        string   `json:"role"`
	Sectors     []string `json:"sectors"`
	ExpiresAtMs int64    `json:"expires_at_ms"`
}

// loginHandler: POST /api/login {username, password}
//
//	200 {token, ...}   400 bad JSON   401 wrong username or password   404 login off
func (g *gateway) loginHandler(w http.ResponseWriter, r *http.Request) {
	if g.auth == nil {
		writeErr(w, http.StatusNotFound, "login is off (AUTH=off)")
		return
	}
	var req loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad JSON")
		return
	}
	// bcrypt is slow on purpose, so do not hold g.mu while checking.
	tok, c, err := g.auth.login(req.Username, req.Password)

	g.mu.Lock()
	if err != nil {
		g.sec.FailedLogins++
	} else {
		g.sec.Logins++
	}
	g.mu.Unlock()

	if err != nil {
		log.Printf("login failed for %q", req.Username)
		writeErr(w, http.StatusUnauthorized, err.Error())
		return
	}
	log.Printf("login %s (%s %v)", c.Subject, c.Role, c.Sectors)
	writeJSONStatus(w, http.StatusOK, loginResponse{
		Token: tok, Username: c.Subject, Role: c.Role, Sectors: c.Sectors,
		ExpiresAtMs: c.ExpiresAt.UnixMilli(),
	})
}

// authenticate returns the caller's scope for a clearance. With login off it
// returns (nil, true). Otherwise it answers 401 itself and returns false.
func (g *gateway) authenticate(w http.ResponseWriter, r *http.Request) (*claims, bool) {
	if g.auth == nil {
		return nil, true
	}
	c, err := g.auth.verify(bearer(r))
	if err != nil {
		g.mu.Lock()
		g.sec.Unauthorized++
		g.mu.Unlock()
		w.Header().Set("WWW-Authenticate", `Bearer realm="atc"`)
		writeErr(w, http.StatusUnauthorized, "log in first: "+err.Error())
		return nil, false
	}
	return c, true
}
