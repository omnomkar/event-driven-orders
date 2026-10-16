// Package e2e holds end-to-end tests that run the whole Compose stack and
// drive it through the gateway's public REST API. The tests are compiled only
// with the integration build tag:
//
//	go test -tags integration ./...
//
// They use the same host ports as the local stack, so stop it first with
// make down.
package e2e
