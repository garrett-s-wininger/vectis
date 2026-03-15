package interfaces_test

import (
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
)

func TestMockRegistryClient_Register(t *testing.T) {
	client := mocks.NewMockRegistryClient()

	err := client.Register(context.Background(), api.Component_COMPONENT_QUEUE, "localhost:8081")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	addr, ok := client.GetRegistration(api.Component_COMPONENT_QUEUE)
	if !ok {
		t.Error("expected registration to be stored")
	}

	if addr != "localhost:8081" {
		t.Errorf("expected address 'localhost:8081', got '%s'", addr)
	}
}

func TestMockRegistryClient_RegisterError(t *testing.T) {
	client := mocks.NewMockRegistryClient()
	expectedErr := errors.New("registration failed")
	client.SetRegisterError(expectedErr)

	err := client.Register(context.Background(), api.Component_COMPONENT_QUEUE, "localhost:8081")
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockRegistryClient_Address(t *testing.T) {
	client := mocks.NewMockRegistryClient()
	client.SetAddress(api.Component_COMPONENT_QUEUE, "localhost:8081")

	addr, err := client.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if addr != "localhost:8081" {
		t.Errorf("expected address 'localhost:8081', got '%s'", addr)
	}
}

func TestMockRegistryClient_AddressNotFound(t *testing.T) {
	client := mocks.NewMockRegistryClient()

	_, err := client.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err == nil {
		t.Error("expected error for unknown component")
	}
}

func TestMockRegistryClient_AddressError(t *testing.T) {
	client := mocks.NewMockRegistryClient()
	expectedErr := errors.New("address lookup failed")
	client.SetAddressError(expectedErr)

	_, err := client.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestMockRegistryClient_Close(t *testing.T) {
	client := mocks.NewMockRegistryClient()

	if client.IsClosed() {
		t.Error("expected client to be open initially")
	}

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if !client.IsClosed() {
		t.Error("expected client to be closed")
	}
}

func TestMockRegistryClient_MultipleComponents(t *testing.T) {
	client := mocks.NewMockRegistryClient()

	components := map[api.Component]string{
		api.Component_COMPONENT_QUEUE: "localhost:8081",
		api.Component_COMPONENT_LOG:   "localhost:8084",
	}

	for component, address := range components {
		err := client.Register(context.Background(), component, address)
		if err != nil {
			t.Errorf("failed to register %s: %v", component.String(), err)
		}
	}

	for component, expectedAddr := range components {
		addr, ok := client.GetRegistration(component)
		if !ok {
			t.Errorf("registration not found for %s", component.String())
			continue
		}

		if addr != expectedAddr {
			t.Errorf("expected address '%s' for %s, got '%s'", expectedAddr, component.String(), addr)
		}
	}
}
