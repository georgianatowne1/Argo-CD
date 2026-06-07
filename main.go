package main

import (
	"fmt"
	"sync"
	"time"
)

// HealthStatus represents the health status of a resource or application
type HealthStatus string

const (
	HealthStatusProgressing HealthStatus = "Progressing"
	HealthStatusHealthy     HealthStatus = "Healthy"
	HealthStatusDegraded    HealthStatus = "Degraded"
	HealthStatusUnknown     HealthStatus = "Unknown"
)

// Resource represents a managed Kubernetes resource
type Resource struct {
	Name   string
	Status HealthStatus
}

// Application represents an Argo CD Application
type Application struct {
	Name   string
	Status HealthStatus
	mu     sync.Mutex
}

// ApplicationController manages the reconciliation of Applications
type ApplicationController struct {
	queue      chan string
	apps       map[string]*Application
	resources  map[string][]Resource
	mu         sync.RWMutex
	requeueMap map[string]time.Time
}

func NewApplicationController() *ApplicationController {
	return &ApplicationController{
		queue:      make(chan string, 100),
		apps:       make(map[string]*Application),
		resources:  make(map[string][]Resource),
		requeueMap: make(map[string]time.Time),
	}
}

// Reconcile performs the reconciliation loop for a given application
func (c *ApplicationController) Reconcile(appName string) {
	c.mu.Lock()
	app, exists := c.apps[appName]
	resources := c.resources[appName]
	c.mu.Unlock()

	if !exists {
		return
	}

	app.mu.Lock()
	defer app.mu.Unlock()

	fmt.Printf("Reconciling Application: %s (Current Status: %s)\n", app.Name, app.Status)

	allHealthy := true
	anyDegraded := false
	anyProgressing := false
	anyUnknown := false

	for _, res := range resources {
		switch res.Status {
		case HealthStatusHealthy:
			// Healthy, do nothing
		case HealthStatusDegraded:
			anyDegraded = true
			allHealthy = false
		case HealthStatusProgressing:
			anyProgressing = true
			allHealthy = false
		case HealthStatusUnknown:
			anyUnknown = true
			allHealthy = false
		default:
			anyUnknown = true
			allHealthy = false
		}
	}

	var targetStatus HealthStatus
	if anyDegraded {
		targetStatus = HealthStatusDegraded
	} else if anyProgressing {
		targetStatus = HealthStatusProgressing
	} else if anyUnknown {
		// If there is an unknown/transient state, we keep progressing but enqueue a re-reconciliation with backoff
		targetStatus = HealthStatusProgressing
		c.enqueueWithBackoff(appName)
	} else if allHealthy {
		targetStatus = HealthStatusHealthy
	} else {
		targetStatus = HealthStatusProgressing
	}

	if app.Status != targetStatus {
		fmt.Printf("Application %s transitioning from %s to %s\n", app.Name, app.Status, targetStatus)
		app.Status = targetStatus
	}
}

func (c *ApplicationController) enqueueWithBackoff(appName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Simple backoff mechanism to avoid stalling
	now := time.Now()
	if lastRequeue, exists := c.requeueMap[appName]; exists && now.Sub(lastRequeue) < 100*time.Millisecond {
		return // Avoid rapid requeuing
	}
	c.requeueMap[appName] = now

	go func() {
		time.Sleep(100 * time.Millisecond)
		c.queue <- appName
	}()
}

func main() {
	fmt.Println("Starting Argo CD Application Controller Simulation...")

	controller := NewApplicationController()

	// Setup a test application
	appName := "guestbook"
	app := &Application{
		Name:   appName,
		Status: HealthStatusProgressing,
	}

	controller.apps[appName] = app
	controller.resources[appName] = []Resource{
		{Name: "guestbook-ui-deployment", Status: HealthStatusProgressing},
		{Name: "guestbook-ui-service", Status: HealthStatusHealthy},
	}

	// Start reconciliation worker
	go func() {
		for appName := range controller.queue {
			controller.Reconcile(appName)
		}
	}()

	// Initial reconcile
	controller.queue <- appName
	time.Sleep(200 * time.Millisecond)

	// Simulate resource transitioning to Healthy
	fmt.Println("\nSimulating resource transition to Healthy...")
	controller.mu.Lock()
	controller.resources[appName] = []Resource{
		{Name: "guestbook-ui-deployment", Status: HealthStatusHealthy},
		{Name: "guestbook-ui-service", Status: HealthStatusHealthy},
	}
	controller.mu.Unlock()

	controller.queue <- appName
	time.Sleep(200 * time.Millisecond)

	// Verify final status
	app.mu.Lock()
	finalStatus := app.Status
	app.mu.Unlock()

	fmt.Printf("\nFinal Application Status: %s\n", finalStatus)
	if finalStatus == HealthStatusHealthy {
		fmt.Println("SUCCESS: Application successfully transitioned to Healthy!")
	} else {
		fmt.Println("FAILURE: Application stuck in Progressing!")
	}
}
