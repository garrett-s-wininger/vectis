package api

import "golang.org/x/crypto/bcrypt"

var passwordHashCost = bcrypt.DefaultCost

func generatePasswordHash(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), passwordHashCost)
}
