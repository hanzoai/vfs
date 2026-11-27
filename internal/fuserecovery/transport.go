package fuserecovery

import (
	"fmt"
	"sync"
)

// PendingRequest represents an in-flight or kernel-queued FUSE request held by
// the /dev/fuse channel while a crashed daemon is being respawned.
type PendingRequest struct {
	ReqID  uint64
	Op     string
	Tenant string
	Path   string
	Inode  uint64
	Handle uint64
	Offset int64
	Data   []byte
	Flags  uint32
	Mode   uint32
}

// FuseChannel abstracts the supervisor-held /dev/fuse file descriptor (or
// equivalent transport channel) that decouples the kernel mount lifetime from
// the userspace FUSE daemon process lifetime.
type FuseChannel interface {
	SessionID() string
	MountPath() string
	MountAlive() bool
	HandoffAvailable() bool
	CurrentGeneration() uint64
	AttachDaemon(gen uint64) error
	DetachOnCrash()
	EnqueueBlocked(req PendingRequest) error
	DrainBlocked() []PendingRequest
	SimulateKernelCrash()
	DisableHandoff()
	ColdRemount(newSessionID string) error
	UnmountCount() uint64
}

// SupervisorChannel models a Linux /dev/fuse file descriptor owned by a tiny,
// restart-resilient supervisor process and inherited/passed (e.g. via
// SCM_RIGHTS) to the worker FUSE daemon. Because the supervisor keeps the fd
// open across a daemon SIGKILL, the Linux FUSE kernel module keeps the mount
// active and queues requests instead of returning ENOTCONN.
type SupervisorChannel struct {
	mu               sync.Mutex
	sessionID        string
	mountPath        string
	fdToken          int
	generation       uint64
	mountAlive       bool
	handoffSupported bool
	daemonAttached   bool
	unmountCount     uint64
	blocked          []PendingRequest
}

// NewSupervisorChannel creates an active FUSE transport channel with fd handoff
// enabled by default.
func NewSupervisorChannel(sessionID, mountPath string) *SupervisorChannel {
	return &SupervisorChannel{
		sessionID:        sessionID,
		mountPath:        mountPath,
		fdToken:          3, // synthetic inherited /dev/fuse fd slot
		generation:       0,
		mountAlive:       true,
		handoffSupported: true,
	}
}

func (c *SupervisorChannel) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *SupervisorChannel) MountPath() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mountPath
}

func (c *SupervisorChannel) MountAlive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mountAlive
}

func (c *SupervisorChannel) HandoffAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mountAlive && c.handoffSupported
}

func (c *SupervisorChannel) CurrentGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *SupervisorChannel) UnmountCount() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.unmountCount
}

// AttachDaemon hands the live /dev/fuse channel to a daemon of generation gen
// without unmounting the mountpoint.
func (c *SupervisorChannel) AttachDaemon(gen uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mountAlive {
		return fmt.Errorf("%w: kernel mount %q is no longer connected", ErrHandoffUnavailable, c.mountPath)
	}
	if c.generation > 0 && !c.handoffSupported {
		return fmt.Errorf("%w: /dev/fuse fd handoff disabled for %q", ErrHandoffUnavailable, c.mountPath)
	}
	c.generation = gen
	c.daemonAttached = true
	return nil
}

// DetachOnCrash models an abrupt daemon SIGKILL. If the supervisor holds the
// /dev/fuse fd (handoffSupported == true), the kernel mount remains alive and
// queues subsequent FUSE requests. If the daemon exclusively owned /dev/fuse
// (handoffSupported == false), the mount tears down immediately.
func (c *SupervisorChannel) DetachOnCrash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.daemonAttached = false
	if !c.handoffSupported && c.mountAlive {
		c.mountAlive = false
		c.unmountCount++
		c.blocked = nil
	}
}

// EnqueueBlocked buffers a FUSE request while the daemon is mid-restart.
func (c *SupervisorChannel) EnqueueBlocked(req PendingRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mountAlive {
		return fmt.Errorf("%w: mount %q disconnected", ErrHandoffUnavailable, c.mountPath)
	}
	cp := req
	if req.Data != nil {
		cp.Data = append([]byte(nil), req.Data...)
	}
	c.blocked = append(c.blocked, cp)
	return nil
}

// DrainBlocked atomically returns and clears queued FUSE requests for replay
// by the newly attached daemon.
func (c *SupervisorChannel) DrainBlocked() []PendingRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.blocked) == 0 {
		return nil
	}
	out := c.blocked
	c.blocked = nil
	return out
}

// SimulateKernelCrash models a full host/kernel crash where both the daemon
// and the supervisor-held /dev/fuse descriptor are lost, requiring cold remount.
func (c *SupervisorChannel) SimulateKernelCrash() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mountAlive {
		c.unmountCount++
	}
	c.mountAlive = false
	c.handoffSupported = false
	c.daemonAttached = false
	c.blocked = nil
}

// DisableHandoff simulates an environment where /dev/fuse fd passing is not
// supported by the supervisor or host kernel.
func (c *SupervisorChannel) DisableHandoff() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handoffSupported = false
}

// ColdRemount tears down any existing mount session and establishes a brand-new
// mount session (invalidating kernel dcache and all open handles).
func (c *SupervisorChannel) ColdRemount(newSessionID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mountAlive {
		c.unmountCount++
	}
	c.sessionID = newSessionID
	c.fdToken++
	c.generation = 0
	c.mountAlive = true
	c.daemonAttached = false
	c.blocked = nil
	return nil
}
