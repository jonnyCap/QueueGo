package auth

import (
	"errors"
	"fmt"
	"sync"

	"github.com/golang-jwt/jwt/v5"
)

var (
	masterKeyMu sync.RWMutex
	masterKey   string
)

func SetMasterKey(key string) {
	masterKeyMu.Lock()
	defer masterKeyMu.Unlock()
	masterKey = key
}

func GetMasterKey() string {
	masterKeyMu.RLock()
	defer masterKeyMu.RUnlock()
	return masterKey
}

// Claims represents the standard Blink JWT claims.
type Claims struct {
	jwt.RegisteredClaims
	QueueKey    string   `json:"queue_key"`
	Permissions []string `json:"permissions"`
}

// ParseToken verifies and parses a JWT using the master key.
func ParseToken(tokenBytes []byte) (*Claims, error) {
	key := GetMasterKey()
	if key == "" {
		return nil, errors.New("master key is not configured")
	}

	tokenStr := string(tokenBytes)
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return []byte(key), nil
	})
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}

	if !token.Valid {
		return nil, errors.New("token is not valid")
	}

	mapClaims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims format")
	}

	claims := &Claims{}
	if sub, ok := mapClaims["sub"].(string); ok {
		claims.Subject = sub
	}
	if qk, ok := mapClaims["queue_key"].(string); ok {
		claims.QueueKey = qk
	}

	if permsRaw, ok := mapClaims["permissions"]; ok {
		switch p := permsRaw.(type) {
		case []interface{}:
			for _, item := range p {
				if s, ok := item.(string); ok {
					claims.Permissions = append(claims.Permissions, s)
				}
			}
		case []string:
			claims.Permissions = append(claims.Permissions, p...)
		case string:
			claims.Permissions = append(claims.Permissions, p)
		}
	}

	return claims, nil
}

// VerifyCreate verifies the JWT for CREATE and extracts the queue_key.
func VerifyCreate(token []byte) (string, error) {
	claims, err := ParseToken(token)
	if err != nil {
		return "", err
	}
	if claims.QueueKey == "" {
		return "", errors.New("missing queue_key claim in token")
	}
	return claims.QueueKey, nil
}

// VerifyTopicAccess checks that the token is valid with masterKey, matches the topic's queue_key, and has the required permission.
func VerifyTopicAccess(token []byte, expectedQueueKey string, requiredPermission string) error {
	claims, err := ParseToken(token)
	if err != nil {
		return err
	}

	if claims.QueueKey != expectedQueueKey {
		return errors.New("invalid queue_key for topic")
	}

	if requiredPermission != "" {
		hasPerm := false
		for _, p := range claims.Permissions {
			if p == requiredPermission || p == "*" || p == "admin" {
				hasPerm = true
				break
			}
		}
		if !hasPerm {
			return fmt.Errorf("missing required permission: %s", requiredPermission)
		}
	}

	return nil
}

// GenerateToken is a helper function to create signed JWTs for testing, backend, and clients.
func GenerateToken(signingKey string, sub string, queueKey string, permissions ...string) (string, error) {
	claims := jwt.MapClaims{
		"sub":         sub,
		"queue_key":   queueKey,
		"permissions": permissions,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(signingKey))
}

