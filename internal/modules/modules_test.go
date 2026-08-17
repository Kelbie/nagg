package modules

import "testing"

func TestParseEmptyIsAll(t *testing.T) {
	for _, input := range []string{"", "   ", ",,", " , "} {
		set, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", input, err)
		}
		if !set.IsAll() {
			t.Fatalf("Parse(%q) = %s, want every module", input, set)
		}
	}
}

func TestParseSubset(t *testing.T) {
	set, err := Parse(" Mint , app ")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !set.Has(Mint) || !set.Has(App) {
		t.Fatalf("Parse dropped a named module: %s", set)
	}
	if set.Has(Nostr) {
		t.Fatal("Parse enabled nostr, which was not named")
	}
	if set.IsAll() {
		t.Fatal("a two-module set must not report IsAll")
	}
	if got, want := set.String(), "app,mint"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

// An unknown name must fail loudly: a typo that silently fell back to "all"
// would deploy the full Nostr archive onto a mint-sized machine.
func TestParseUnknownModuleErrors(t *testing.T) {
	if _, err := Parse("mint,nostrr"); err == nil {
		t.Fatal("Parse accepted an unknown module name")
	}
	// "core" is not selectable — it is what remains after subtracting modules.
	if _, err := Parse("core"); err == nil {
		t.Fatal("Parse accepted the core pseudo-module")
	}
}

// The zero value is the compatibility contract: every caller that predates this
// package must keep seeing every module enabled.
func TestZeroSetEnablesEverything(t *testing.T) {
	var set Set
	for _, m := range append([]Module{Core}, known...) {
		if !set.Has(m) {
			t.Fatalf("zero Set disabled %q", m)
		}
	}
	if !set.IsAll() {
		t.Fatal("zero Set must report IsAll")
	}
	if got, want := set.String(), "nostr,mint,app"; got != want {
		t.Fatalf("zero Set String() = %q, want %q", got, want)
	}
}

func TestCoreAlwaysEnabled(t *testing.T) {
	set, err := Parse("mint")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if !set.Has(Core) {
		t.Fatal("core must be enabled in every deployment")
	}
}

func TestParseTag(t *testing.T) {
	for _, name := range []string{"core", "nostr", "mint", "app", " Mint "} {
		if _, err := ParseTag(name); err != nil {
			t.Fatalf("ParseTag(%q) error: %v", name, err)
		}
	}
	if _, err := ParseTag("feed"); err == nil {
		t.Fatal("ParseTag accepted an unknown tag")
	}
}
