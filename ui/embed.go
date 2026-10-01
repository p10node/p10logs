// Package ui embeds the single-page console served by the hub.
package ui

import _ "embed"

// Index is the console page.
//
//go:embed index.html
var Index []byte
