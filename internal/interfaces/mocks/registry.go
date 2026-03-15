package mocks

import (
	"context"
	"errors"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type MockRegistryClient struct {
	mu            sync.Mutex
	registrations map[api.Component]string
	addresses     map[api.Component]string
	registerErr   error
	addressErr    error
	closed        bool
}

func NewMockRegistryClient() *MockRegistryClient {
	return &MockRegistryClient{
		registrations: make(map[api.Component]string),
		addresses:     make(map[api.Component]string),
	}
}

func (m *MockRegistryClient) SetAddress(component api.Component, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addresses[component] = address
}

func (m *MockRegistryClient) SetRegisterError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registerErr = err
}

func (m *MockRegistryClient) SetAddressError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addressErr = err
}

func (m *MockRegistryClient) GetRegistration(component api.Component) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	addr, ok := m.registrations[component]
	return addr, ok
}

func (m *MockRegistryClient) IsClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *MockRegistryClient) Register(ctx context.Context, component api.Component, address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.registerErr != nil {
		return m.registerErr
	}

	m.registrations[component] = address
	return nil
}

func (m *MockRegistryClient) Address(ctx context.Context, component api.Component) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.addressErr != nil {
		return "", m.addressErr
	}

	addr, ok := m.addresses[component]
	if !ok {
		return "", errors.New("address not found for component")
	}

	return addr, nil
}

func (m *MockRegistryClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

var _ interfaces.RegistryClient = (*MockRegistryClient)(nil)
