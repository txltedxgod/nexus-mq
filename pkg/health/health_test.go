package health

import (
	"testing"
)

type HealthStatus int

const (
	StatusServing HealthStatus = 1
	StatusNotServing HealthStatus = 2
)

type Checker struct {
	status HealthStatus
}

func NewChecker() *Checker {
	return &Checker{status: StatusServing}
}

func (c *Checker) IsHealthy() bool {
	return c.status == StatusServing
}

func TestHealthChecker(t *testing.T) {
	c := NewChecker()
	if !c.IsHealthy() {
		t.Errorf("expected serving status, got not serving")
	}
}
