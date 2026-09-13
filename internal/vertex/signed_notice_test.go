package vertex

import (
	"errors"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestNoticeErrorRecognisesVertexCreditWording(t *testing.T) {
	cases := map[string]bool{
		"insufficient credits": true,
		"you don't have enough credits to fulfil the request. Send us a DM": true,
		"not enough balance": true,
		"invalid target":     false,
		"rate limited":       false,
	}
	for message, wantCredits := range cases {
		ev := &nostr.Event{Kind: 7000, Tags: nostr.Tags{{"status", "error", message}}}
		err := noticeError(ev)
		if got := errors.Is(err, ErrInsufficientCredits); got != wantCredits {
			t.Fatalf("%q: insufficient=%v want %v (err=%v)", message, got, wantCredits, err)
		}
	}
}
