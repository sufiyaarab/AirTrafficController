// Command hashpw prints a bcrypt hash of a password, for deploy/users.json.
//
//	echo -n 'my-password' | go run ./cmd/hashpw
//
// The password is read from stdin rather than taken as an argument, so it
// does not end up in your shell history or the process list.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

func main() {
	line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	pw := strings.TrimRight(line, "\r\n")
	if pw == "" {
		fmt.Fprintln(os.Stderr, "usage: echo -n 'PASSWORD' | go run ./cmd/hashpw")
		os.Exit(2)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil { // e.g. longer than bcrypt's 72-byte limit
		fmt.Fprintln(os.Stderr, "hashpw:", err)
		os.Exit(1)
	}
	fmt.Println(string(hash))
}
