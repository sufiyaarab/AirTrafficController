package main

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"atc/internal/fsm"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

func hash(t *testing.T, pw string) string {
	t.Helper()
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.MinCost) // MinCost keeps tests fast
	if err != nil {
		t.Fatal(err)
	}
	return string(h)
}

func testUsers(t *testing.T) []user {
	return []user{
		{Username: "ctrl-sector1", Bcrypt: hash(t, "pw1"), Role: RoleController, Sectors: []string{"sector-1"}},
		{Username: "ctrl-sector2", Bcrypt: hash(t, "pw2"), Role: RoleController, Sectors: []string{"sector-2"}},
		{Username: "supervisor", Bcrypt: hash(t, "pws"), Role: RoleSupervisor},
	}
}

func newTestAuth(t *testing.T) *authenticator {
	t.Helper()
	a, err := newAuthenticator(testUsers(t), testSecret)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestNewAuthenticatorValidates(t *testing.T) {
	good := testUsers(t)[0]
	cases := map[string]func(u *user){
		"bad role":            func(u *user) { u.Role = "admin" },
		"controller no scope": func(u *user) { u.Sectors = nil },
		"plain password":      func(u *user) { u.Bcrypt = "pw1" },
		"empty username":      func(u *user) { u.Username = " " },
	}
	for name, mutate := range cases {
		u := good
		mutate(&u)
		if _, err := newAuthenticator([]user{u}, testSecret); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := newAuthenticator([]user{good, good}, testSecret); err == nil {
		t.Error("duplicate username accepted")
	}
	if _, err := newAuthenticator([]user{good}, []byte("short")); err == nil {
		t.Error("short secret accepted")
	}
}

func TestLoginAndVerify(t *testing.T) {
	a := newTestAuth(t)

	tok, c, err := a.login("ctrl-sector1", "pw1")
	if err != nil || tok == "" || c.Subject != "ctrl-sector1" {
		t.Fatalf("login: %v %+v", err, c)
	}
	got, err := a.verify(tok)
	if err != nil || got.Role != RoleController || len(got.Sectors) != 1 || got.Sectors[0] != "sector-1" {
		t.Fatalf("verify: %v %+v", err, got)
	}

	for _, bad := range [][2]string{{"ctrl-sector1", "wrong"}, {"nobody", "pw1"}, {"ctrl-sector1", ""}} {
		if _, _, err := a.login(bad[0], bad[1]); err != errBadCredentials {
			t.Errorf("login %v: got %v", bad, err)
		}
	}
}

func TestVerifyRejectsForgedTokens(t *testing.T) {
	a := newTestAuth(t)
	tok, _, _ := a.login("ctrl-sector1", "pw1")

	// Edit the claims to add sector-2, keep the old signature.
	parts := strings.Split(tok, ".")
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1]) // anyone can read the claims...
	edited := strings.Replace(string(payload), `"sector-1"`, `"sector-1","sector-2"`, 1)
	forged := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(edited)) + "." + parts[2]
	if !strings.Contains(edited, "sector-2") {
		t.Fatal("test did not edit the claims")
	}
	if _, err := a.verify(forged); err == nil { // ...but not change them
		t.Error("token with edited claims accepted")
	}

	// Signed with a different secret.
	other, _ := newAuthenticator(testUsers(t), []byte("ffffffffffffffffffffffffffffffff"))
	tok2, _, _ := other.login("ctrl-sector1", "pw1")
	if _, err := a.verify(tok2); err == nil {
		t.Error("token from another secret accepted")
	}

	// "alg": "none" (unsigned).
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, &claims{Role: RoleSupervisor,
		RegisteredClaims: jwt.RegisteredClaims{Subject: "supervisor", ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))},
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := a.verify(none); err == nil {
		t.Error("unsigned token accepted")
	}

	// Garbage and empty.
	for _, s := range []string{"", "abc", "a.b.c"} {
		if _, err := a.verify(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestTokenExpires(t *testing.T) {
	a := newTestAuth(t)
	t0 := time.Now()
	a.now = func() time.Time { return t0 }
	tok, _, _ := a.login("ctrl-sector1", "pw1")

	a.now = func() time.Time { return t0.Add(tokenTTL - time.Minute) }
	if _, err := a.verify(tok); err != nil {
		t.Fatalf("still valid: %v", err)
	}
	a.now = func() time.Time { return t0.Add(tokenTTL + time.Minute) }
	if _, err := a.verify(tok); err == nil {
		t.Fatal("expired token accepted")
	}
}

func TestTokenForRemovedUser(t *testing.T) {
	a := newTestAuth(t)
	tok, _, _ := a.login("ctrl-sector2", "pw2")
	delete(a.users, "ctrl-sector2") // account removed from users.json and gateway restarted
	if _, err := a.verify(tok); err == nil {
		t.Fatal("token of a removed user accepted")
	}
}

func TestBearer(t *testing.T) {
	cases := map[string]string{"Bearer abc": "abc", "bearer  abc ": "abc", "Basic abc": "", "": "", "Bearer": ""}
	for h, want := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		if h != "" {
			r.Header.Set("Authorization", h)
		}
		if got := bearer(r); got != want {
			t.Errorf("%q: got %q want %q", h, got, want)
		}
	}
}

// ---------- through the HTTP handlers ----------

func (h *harness) login(t *testing.T, user, pw string) (int, loginResponse) {
	t.Helper()
	body, _ := json.Marshal(loginRequest{Username: user, Password: pw})
	rec := httptest.NewRecorder()
	h.g.loginHandler(rec, httptest.NewRequest("POST", "/api/login", strings.NewReader(string(body))))
	var res loginResponse
	json.NewDecoder(rec.Body).Decode(&res)
	return rec.Code, res
}

func (h *harness) postAs(t *testing.T, token, callsign, instruction, issuedBy string) (int, clearance) {
	t.Helper()
	body := `{"callsign":"` + callsign + `","instruction":"` + instruction + `","issued_by":"` + issuedBy + `"}`
	req := httptest.NewRequest("POST", "/api/clearance", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.g.clearanceHandler(rec, req)
	var c clearance
	json.NewDecoder(rec.Body).Decode(&c)
	return rec.Code, c
}

func newAuthHarness(t *testing.T) *harness {
	h := newHarness(t)
	h.g.auth = newTestAuth(t)
	h.own("ATC001", "sector-1", fsm.Active)
	h.own("ATC002", "sector-2", fsm.Active)
	return h
}

func TestLoginHandler(t *testing.T) {
	h := newAuthHarness(t)
	code, res := h.login(t, "ctrl-sector1", "pw1")
	if code != http.StatusOK || res.Token == "" || res.Role != RoleController || res.ExpiresAtMs <= time.Now().UnixMilli() {
		t.Fatalf("got %d %+v", code, res)
	}
	if code, _ := h.login(t, "ctrl-sector1", "nope"); code != http.StatusUnauthorized {
		t.Fatalf("wrong password: got %d", code)
	}
	if h.g.sec.Logins != 1 || h.g.sec.FailedLogins != 1 {
		t.Fatalf("stats %+v", h.g.sec)
	}
}

func TestClearanceNeedsToken(t *testing.T) {
	h := newAuthHarness(t)
	for _, tok := range []string{"", "not-a-token"} {
		if code, _ := h.postAs(t, tok, "ATC001", "CLIMB FL300", "ctrl-sector1"); code != http.StatusUnauthorized {
			t.Errorf("token %q: got %d, want 401", tok, code)
		}
	}
	if len(h.sectors["sector-1"].applied) != 0 || h.g.sec.Unauthorized != 2 {
		t.Fatalf("applied %d, stats %+v", len(h.sectors["sector-1"].applied), h.g.sec)
	}
}

func TestControllerOnlyClearsOwnSector(t *testing.T) {
	h := newAuthHarness(t)
	_, res := h.login(t, "ctrl-sector1", "pw1")

	// Own sector: committed, and issued_by comes from the token, not the body.
	code, c := h.postAs(t, res.Token, "ATC001", "CLIMB FL300", "someone-else")
	if code != http.StatusOK || c.IssuedBy != "ctrl-sector1" {
		t.Fatalf("own sector: got %d %+v", code, c)
	}
	// Another controller's sector.
	code, c = h.postAs(t, res.Token, "ATC002", "CLIMB FL300", "")
	if code != http.StatusForbidden || !strings.Contains(c.Reason, "sector-2") {
		t.Fatalf("other sector: got %d %+v", code, c)
	}
	if len(h.sectors["sector-2"].applied) != 0 || h.g.sec.Forbidden != 1 {
		t.Fatalf("nothing may reach sector-2; stats %+v", h.g.sec)
	}
}

func TestSupervisorClearsAnySector(t *testing.T) {
	h := newAuthHarness(t)
	_, res := h.login(t, "supervisor", "pws")
	for _, cs := range []string{"ATC001", "ATC002"} {
		if code, c := h.postAs(t, res.Token, cs, "HEADING 090", ""); code != http.StatusOK || c.IssuedBy != "supervisor" {
			t.Fatalf("%s: got %d %+v", cs, code, c)
		}
	}
}

// A buffered clearance is checked again when it is flushed: if the aircraft
// moved to a sector the controller does not hold, it is refused, never
// committed in the new sector.
func TestBufferedClearanceRecheckedOnFlush(t *testing.T) {
	h := newAuthHarness(t)
	_, res := h.login(t, "ctrl-sector1", "pw1")
	h.setLeader("sector-1", false)
	if code, _ := h.postAs(t, res.Token, "ATC001", "DESCEND FL100", ""); code != http.StatusAccepted {
		t.Fatalf("want pending, got %d", code)
	}
	h.own("ATC001", "sector-2", fsm.Active) // handed off while waiting
	h.setLeader("sector-1", true)
	h.g.flushQueue("sector-1")
	if c := h.g.clr.recent[0]; c.Status != ClrRejected {
		t.Fatalf("want rejected, got %+v", c)
	}
	if len(h.sectors["sector-2"].applied) != 0 {
		t.Fatal("committed in a sector the controller does not hold")
	}
}

func TestLoginOff(t *testing.T) {
	h := newHarness(t) // auth nil: clearances work as before login existed
	h.own("ATC001", "sector-1", fsm.Active)
	if code, c := h.postAs(t, "", "ATC001", "CLIMB FL300", "ctrl-demo"); code != http.StatusOK || c.IssuedBy != "ctrl-demo" {
		t.Fatalf("got %d %+v", code, c)
	}
	if code, _ := h.login(t, "x", "y"); code != http.StatusNotFound {
		t.Fatalf("login with auth off: got %d", code)
	}
}

// The demo accounts in deploy/users.json load and their documented demo
// passwords (README) work.
func TestDemoUsersFile(t *testing.T) {
	t.Setenv("USERS_FILE", "../../deploy/users.json")
	t.Setenv("JWT_SECRET", string(testSecret))
	a := authFromEnv()
	for user, pw := range map[string]string{"ctrl-sector1": "sector1-demo", "ctrl-tower": "tower-demo", "supervisor": "supervisor-demo"} {
		if _, _, err := a.login(user, pw); err != nil {
			t.Errorf("%s: %v", user, err)
		}
	}
}
