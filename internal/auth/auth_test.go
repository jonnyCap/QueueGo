package auth

import (
	"testing"
)

func TestAuthWorkflow(t *testing.T) {
	masterKey := "test-master-secret-key-12345"
	SetMasterKey(masterKey)

	// Generate create token
	createToken, err := GenerateToken(masterKey, "admin-user", "queue-secret-abc", "create")
	if err != nil {
		t.Fatalf("failed to generate create token: %v", err)
	}

	queueKey, err := VerifyCreate([]byte(createToken))
	if err != nil {
		t.Fatalf("VerifyCreate failed: %v", err)
	}
	if queueKey != "queue-secret-abc" {
		t.Fatalf("expected queue_key 'queue-secret-abc', got %q", queueKey)
	}

	// Generate token with subscribe and publish permissions
	clientToken, err := GenerateToken(masterKey, "sub-pub-user", "queue-secret-abc", "subscribe", "publish")
	if err != nil {
		t.Fatalf("failed to generate client token: %v", err)
	}

	// Verify valid subscribe access
	if err := VerifyTopicAccess([]byte(clientToken), "queue-secret-abc", "subscribe"); err != nil {
		t.Fatalf("expected subscribe access to succeed, got: %v", err)
	}

	// Verify valid publish access
	if err := VerifyTopicAccess([]byte(clientToken), "queue-secret-abc", "publish"); err != nil {
		t.Fatalf("expected publish access to succeed, got: %v", err)
	}

	// Verify rotate access denied (permission missing)
	if err := VerifyTopicAccess([]byte(clientToken), "queue-secret-abc", "rotate"); err == nil {
		t.Fatalf("expected rotate access to be rejected for missing permission")
	}

	// Verify wrong queue key denied
	if err := VerifyTopicAccess([]byte(clientToken), "wrong-queue-key", "subscribe"); err == nil {
		t.Fatalf("expected access with wrong queue_key to be rejected")
	}

	// Verify token signed with wrong master key
	wrongMasterToken, err := GenerateToken("wrong-master-key", "user", "queue-secret-abc", "subscribe")
	if err != nil {
		t.Fatalf("failed to generate wrong token: %v", err)
	}
	if err := VerifyTopicAccess([]byte(wrongMasterToken), "queue-secret-abc", "subscribe"); err == nil {
		t.Fatalf("expected token signed with wrong master key to be rejected")
	}
}
