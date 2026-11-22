package auth

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

// User Tiers
const (
	TierGuest = "guest"
	TierFree  = "free"
	TierPaid  = "paid"
	TierAdmin = "admin"
)

// Claims defines the JWT token payload
type Claims struct {
	UserID string `json:"user_id"`
	Tier   string `json:"tier"` // guest, free, paid, or admin
	jwt.RegisteredClaims
}

type AuthManager struct {
	jwtSecret    []byte
	aesMasterKey string
	adminUser    string
	adminPass    string
}

func NewAuthManager(jwtSecret, aesMasterKey string) *AuthManager {
	return &AuthManager{
		jwtSecret:    []byte(jwtSecret),
		aesMasterKey: aesMasterKey,
	}
}

// SetAdminCredentials configures the admin username and password from config
func (a *AuthManager) SetAdminCredentials(username, password string) {
	a.adminUser = username
	a.adminPass = password
}

// HashPassword hashes a plaintext password with bcrypt
func (a *AuthManager) HashPassword(password string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// CheckPassword verifies a plaintext password against a stored bcrypt hash
func (a *AuthManager) CheckPassword(hash, password string) bool {
	if hash == "" || password == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) == nil
}

// CheckAdminCredentials verifies admin username and password in constant time
func (a *AuthManager) CheckAdminCredentials(username, password string) bool {
	if a.adminUser == "" || a.adminPass == "" {
		return false
	}
	userMatch := subtle.ConstantTimeCompare([]byte(username), []byte(a.adminUser)) == 1
	passMatch := subtle.ConstantTimeCompare([]byte(password), []byte(a.adminPass)) == 1
	return userMatch && passMatch
}

// GenerateToken creates a signed JWT token for a user with specific tier access
func (a *AuthManager) GenerateToken(userID string, tier string, durationHours int) (string, error) {
	expirationTime := time.Now().Add(time.Duration(durationHours) * time.Hour)
	claims := &Claims{
		UserID: userID,
		Tier:   tier,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Subject:   userID,
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(a.jwtSecret)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return tokenString, nil
}

// ValidateToken verifies and parses a JWT token
func (a *AuthManager) ValidateToken(tokenStr string) (*Claims, error) {
	if tokenStr == "" {
		// Default to Guest Tier
		return &Claims{UserID: "guest_anon", Tier: TierGuest}, nil
	}

	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return a.jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return nil, errors.New("invalid or expired authorization token")
	}

	return claims, nil
}

// ValidateAdminToken verifies that a JWT belongs to an authenticated admin
func (a *AuthManager) ValidateAdminToken(tokenStr string) (*Claims, error) {
	if tokenStr == "" {
		return nil, errors.New("missing admin token")
	}
	claims, err := a.ValidateToken(tokenStr)
	if err != nil {
		return nil, err
	}
	if claims.Tier != TierAdmin {
		return nil, errors.New("insufficient privileges: admin role required")
	}
	return claims, nil
}

// CanAccessChannel verifies if a user claims can access a specific channel tier
func (a *AuthManager) CanAccessChannel(userTier string, channelTier string) bool {
	if userTier == TierAdmin {
		return true
	}
	if channelTier == TierFree || channelTier == TierGuest || channelTier == "" {
		return true // Free channels accessible to everyone
	}

	if channelTier == TierPaid {
		return userTier == TierPaid
	}

	return false
}

// GetDecryptionKey returns the AES decryption key for Paid tier subscribers
func (a *AuthManager) GetDecryptionKey(userTier string, channelTier string) (string, error) {
	if !a.CanAccessChannel(userTier, channelTier) {
		return "", errors.New("access denied: subscription required for paid channel")
	}

	if channelTier == TierPaid {
		return a.aesMasterKey, nil
	}

	return "", nil // No encryption for free channels
}
