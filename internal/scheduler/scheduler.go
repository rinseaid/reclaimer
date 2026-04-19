package scheduler

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/robfig/cron/v3"

	"github.com/rinseaid/reclaimer/internal/database"
)

type Scheduler struct {
	cron *cron.Cron
	mu   sync.Mutex
	jobs map[string]cron.EntryID
	db   *database.DB
}

func New(db *database.DB) *Scheduler {
	return &Scheduler{
		cron: cron.New(),
		jobs: make(map[string]cron.EntryID),
		db:   db,
	}
}

func (s *Scheduler) Start() {
	s.cron.Start()
}

func (s *Scheduler) Stop() {
	s.cron.Stop()
}

func (s *Scheduler) AddFunc(name, spec string, cmd func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if old, ok := s.jobs[name]; ok {
		s.cron.Remove(old)
	}

	id, err := s.cron.AddFunc(spec, cmd)
	if err != nil {
		return fmt.Errorf("add job %s: %w", name, err)
	}
	s.jobs[name] = id
	slog.Info("scheduled job", "name", name, "cron", spec)
	return nil
}

func (s *Scheduler) Remove(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.jobs[name]; ok {
		s.cron.Remove(id)
		delete(s.jobs, name)
	}
}

// ReloadPerRuleSchedules syncs the cron jobs against collection_config rows
// that have a non-null schedule_cron.
func (s *Scheduler) ReloadPerRuleSchedules(runForRule func(ruleName string)) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, id := range s.jobs {
		if len(name) > 10 && name[:10] == "rule-cron-" {
			s.cron.Remove(id)
			delete(s.jobs, name)
		}
	}

	type row struct {
		ID           int64  `db:"id"`
		Name         string `db:"name"`
		ScheduleCron string `db:"schedule_cron"`
	}
	var rows []row
	err := s.db.Select(&rows, "SELECT id, name, schedule_cron FROM collection_config WHERE schedule_cron IS NOT NULL AND schedule_cron != ''")
	if err != nil {
		slog.Warn("failed to load per-rule schedules", "error", err)
		return
	}

	for _, r := range rows {
		spec := r.ScheduleCron
		jobName := fmt.Sprintf("rule-cron-%d", r.ID)
		ruleName := r.Name
		id, err := s.cron.AddFunc(spec, func() { runForRule(ruleName) })
		if err != nil {
			slog.Warn("invalid cron for rule", "rule", ruleName, "cron", spec, "error", err)
			continue
		}
		s.jobs[jobName] = id
		slog.Info("registered per-rule schedule", "job", jobName, "cron", spec, "rule", ruleName)
	}
}
