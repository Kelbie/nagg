package main

import "testing"

func TestListenAddrPrefersExplicitNaggAddr(t *testing.T) {
	addr := listenAddr(func(key string) string {
		switch key {
		case "NAGG_API_ADDR":
			return "127.0.0.1:9090"
		case "PORT":
			return "8080"
		default:
			return ""
		}
	})
	if addr != "127.0.0.1:9090" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestListenAddrUsesRailwayPort(t *testing.T) {
	addr := listenAddr(func(key string) string {
		if key == "PORT" {
			return "4567"
		}
		return ""
	})
	if addr != ":4567" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestListenAddrDefaultsToLocalPort(t *testing.T) {
	addr := listenAddr(func(string) string { return "" })
	if addr != ":8080" {
		t.Fatalf("addr = %q", addr)
	}
}
