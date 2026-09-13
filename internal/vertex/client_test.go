package vertex

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

type fakeDVMRelay struct {
	request chan nostr.Event
	events  chan *nostr.Event
	filters nostr.Filters
	respond func(nostr.Event) []*nostr.Event
}

func (r *fakeDVMRelay) Subscribe(_ context.Context, f nostr.Filters) (dvmSubscription, error) {
	r.filters = f
	eose := make(chan struct{})
	close(eose)
	return dvmSubscription{events: r.events, eose: eose, closed: make(chan string), close: func() {}}, nil
}
func (r *fakeDVMRelay) Publish(_ context.Context, e nostr.Event) error {
	r.request <- e
	for _, response := range r.respond(e) {
		r.events <- response
	}
	return nil
}
func (r *fakeDVMRelay) Close() error { return nil }
func signedTestEvent(t *testing.T, kind int, tags nostr.Tags, content string) nostr.Event {
	t.Helper()
	e := nostr.Event{Kind: kind, Tags: tags, Content: content, CreatedAt: nostr.Now()}
	if err := e.Sign(nostr.GeneratePrivateKey()); err != nil {
		t.Fatal(err)
	}
	return e
}
func fakeSignedClient(t *testing.T, respond func(nostr.Event) []*nostr.Event) (*Client, *fakeDVMRelay) {
	t.Helper()
	c, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	relay := &fakeDVMRelay{request: make(chan nostr.Event, 1), events: make(chan *nostr.Event, 8), respond: respond}
	c.dial = func(context.Context, string) (dvmRelay, error) { return relay, nil }
	return c, relay
}
func TestSignedRequestForwardedVerbatim(t *testing.T) {
	target := strings.Repeat("a", 64)
	req := signedTestEvent(t, ProfileRequestKind, nostr.Tags{{"param", "target", target}, {"param", "limit", "7"}}, "")
	c, relay := fakeSignedClient(t, func(request nostr.Event) []*nostr.Event {
		e := signedTestEvent(t, ProfileResponseKind, nostr.Tags{{"e", request.ID}, {"nodes", "10000"}}, `[{"pubkey":"`+target+`","rank":0.1}]`)
		return []*nostr.Event{&e}
	})
	result, err := c.RelaySigned(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-relay.request; !reflect.DeepEqual(got, req) {
		t.Fatalf("request mutated: %#v", got)
	}
	if !reflect.DeepEqual(relay.filters, nostr.Filters{{Kinds: []int{6312, 7000}, Tags: nostr.TagMap{"e": []string{req.ID}}}}) {
		t.Fatalf("wrong subscription: %#v", relay.filters)
	}
	if result.Kind != "profile" || result.Profile.PubKey != target || result.Profile.Response == nil || result.Profile.FetchedAt == nil {
		t.Fatalf("bad result: %#v", result)
	}
}
func TestSignedNoticeTypedErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  nostr.Tag
		credits bool
	}{
		{"credits", nostr.Tag{"status", "error", "insufficient credits"}, true},
		{"code", nostr.Tag{"status", "insufficient_credits"}, true},
		{"rejected", nostr.Tag{"status", "error", "private query and control chars\n"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := fakeSignedClient(t, func(req nostr.Event) []*nostr.Event {
				e := signedTestEvent(t, NoticeKind, nostr.Tags{{"e", req.ID}, tc.status}, "")
				return []*nostr.Event{&e}
			})
			req := signedTestEvent(t, SearchRequestKind, nostr.Tags{{"param", "search", "alice"}}, "")
			_, err := c.RelaySigned(context.Background(), req)
			if tc.credits {
				if !errors.Is(err, ErrInsufficientCredits) {
					t.Fatalf("got %v", err)
				}
			} else {
				var rejected *ErrDVMRejected
				if !errors.As(err, &rejected) || strings.Contains(err.Error(), "private") {
					t.Fatalf("unsafe/untyped: %v", err)
				}
			}
		})
	}
}
func TestSignedResponseRejectsInvalidSignatureAndCorrelation(t *testing.T) {
	c, _ := fakeSignedClient(t, func(req nostr.Event) []*nostr.Event {
		wrong := signedTestEvent(t, SearchResponseKind, nostr.Tags{{"e", strings.Repeat("f", 64)}}, `[]`)
		invalid := signedTestEvent(t, SearchResponseKind, nostr.Tags{{"e", req.ID}}, `[]`)
		invalid.Sig = strings.Repeat("0", 128)
		processing := signedTestEvent(t, NoticeKind, nostr.Tags{{"e", req.ID}, {"status", "processing"}}, "")
		valid := signedTestEvent(t, SearchResponseKind, nostr.Tags{{"e", req.ID}}, `[{"pubkey":"`+strings.Repeat("a", 64)+`","rank":0.1}]`)
		return []*nostr.Event{&wrong, &invalid, &processing, &valid}
	})
	req := signedTestEvent(t, SearchRequestKind, nostr.Tags{{"param", "search", "alice"}}, "")
	got, err := c.RelaySigned(context.Background(), req)
	if err != nil || len(got.Results) != 1 {
		t.Fatalf("result=%#v err=%v", got, err)
	}
}
func TestSignedRequestTimeout(t *testing.T) {
	c, _ := fakeSignedClient(t, func(nostr.Event) []*nostr.Event { return nil })
	req := signedTestEvent(t, SearchRequestKind, nostr.Tags{{"param", "search", "alice"}}, "")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.RelaySigned(ctx, req)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
}
func TestServerSignedWrapperStillUsesConfiguredKey(t *testing.T) {
	target := strings.Repeat("a", 64)
	c, relay := fakeSignedClient(t, func(req nostr.Event) []*nostr.Event {
		e := signedTestEvent(t, ProfileResponseKind, nostr.Tags{{"e", req.ID}}, `[{"pubkey":"`+target+`","rank":0.1}]`)
		return []*nostr.Event{&e}
	})
	c.privateKey = nostr.GeneratePrivateKey()
	want, _ := nostr.GetPublicKey(c.privateKey)
	if _, err := c.ProfileRefresh(context.Background(), target); err != nil {
		t.Fatal(err)
	}
	if got := <-relay.request; got.PubKey != want {
		t.Fatalf("got signer %s", got.PubKey)
	}
}
