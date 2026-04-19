package store

import (
	"fmt"

	"github.com/rinseaid/reclaimer/internal/database"
	"github.com/rinseaid/reclaimer/internal/models"
)

type Store struct {
	DB *database.DB
}

func New(db *database.DB) *Store {
	return &Store{DB: db}
}

// --- Collection Config ---

func (s *Store) ListCollectionConfigs() ([]models.CollectionConfig, error) {
	var configs []models.CollectionConfig
	err := s.DB.Select(&configs, "SELECT * FROM collection_config ORDER BY priority ASC, name ASC")
	return configs, err
}

func (s *Store) GetCollectionConfig(name string) (*models.CollectionConfig, error) {
	var cfg models.CollectionConfig
	err := s.DB.Get(&cfg, s.DB.Rebind("SELECT * FROM collection_config WHERE name = ?"), name)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *Store) CreateCollectionConfig(name, mediaType, action string, graceDays int, criteria string, enabled bool, scheduleCron *string, priority int) error {
	_, err := s.DB.Exec(
		s.DB.Rebind(`INSERT INTO collection_config (name, media_type, action, grace_days, criteria, enabled, schedule_cron, priority) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
		name, mediaType, action, graceDays, criteria, enabled, scheduleCron, priority,
	)
	return err
}

func (s *Store) DeleteCollectionConfig(name string) error {
	tx, err := s.DB.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, q := range []string{
		"DELETE FROM collection_config WHERE name = ?",
		"DELETE FROM items WHERE collection = ?",
		"DELETE FROM rule_results WHERE collection = ?",
	} {
		if _, err := tx.Exec(s.DB.Rebind(q), name); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// --- Items ---

func (s *Store) GetItemsByRatingKey(ratingKey string) ([]models.Item, error) {
	var items []models.Item
	err := s.DB.Select(&items, s.DB.Rebind("SELECT * FROM items WHERE rating_key = ?"), ratingKey)
	return items, err
}

func (s *Store) GetCollectionItems(collection, status string, page, perPage int, search, sort, sortDir string) ([]models.Item, int, error) {
	offset := (page - 1) * perPage
	args := []any{collection}

	where := "collection = ?"
	if status != "all" {
		where += " AND status = ?"
		args = append(args, status)
	}
	if search != "" {
		where += " AND title LIKE ?"
		args = append(args, "%"+search+"%")
	}

	validSorts := map[string]bool{"title": true, "first_seen": true, "size_bytes": true, "grace_expires": true}
	if !validSorts[sort] {
		sort = "first_seen"
	}
	if sortDir != "asc" && sortDir != "desc" {
		sortDir = "asc"
	}

	var total int
	countQ := fmt.Sprintf("SELECT COUNT(*) FROM items WHERE %s", where)
	if err := s.DB.Get(&total, s.DB.Rebind(countQ), args...); err != nil {
		return nil, 0, err
	}

	q := fmt.Sprintf("SELECT * FROM items WHERE %s ORDER BY %s %s LIMIT ? OFFSET ?", where, sort, sortDir)
	queryArgs := append(args, perPage, offset)
	var items []models.Item
	if err := s.DB.Select(&items, s.DB.Rebind(q), queryArgs...); err != nil {
		return nil, 0, err
	}

	return items, total, nil
}

func (s *Store) GetRuleResults(ratingKey, collection string) ([]models.RuleResult, error) {
	var results []models.RuleResult
	err := s.DB.Select(&results,
		s.DB.Rebind("SELECT rule_name, passed, detail, severity FROM rule_results WHERE rating_key = ? AND collection = ? ORDER BY rule_name"),
		ratingKey, collection,
	)
	return results, err
}

func (s *Store) SetItemOverride(ratingKey string, override, reason *string) error {
	_, err := s.DB.Exec(
		s.DB.Rebind("UPDATE items SET override = ?, override_by = ? WHERE rating_key = ?"),
		override, reason, ratingKey,
	)
	return err
}

// --- Activity ---

func (s *Store) InsertActivity(eventType, collection, ratingKey, title, detail string) error {
	_, err := s.DB.Exec(
		s.DB.Rebind("INSERT INTO activity_log (event_type, collection, rating_key, title, detail) VALUES (?, ?, ?, ?, ?)"),
		eventType, collection, ratingKey, title, detail,
	)
	return err
}

func (s *Store) GetActivity(eventType, collection string, page, perPage int) ([]models.ActivityLog, int, error) {
	args := []any{}
	where := ""
	clauses := []string{}

	if eventType != "" {
		clauses = append(clauses, "event_type = ?")
		args = append(args, eventType)
	}
	if collection != "" {
		clauses = append(clauses, "(collection = ? OR LOWER(REPLACE(collection, ' ', '-')) = LOWER(?))")
		args = append(args, collection, collection)
	}
	if len(clauses) > 0 {
		where = " WHERE " + joinStr(clauses, " AND ")
	}

	var total int
	if err := s.DB.Get(&total, s.DB.Rebind("SELECT COUNT(*) FROM activity_log"+where), args...); err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * perPage
	queryArgs := append(args, perPage, offset)
	var logs []models.ActivityLog
	q := fmt.Sprintf("SELECT * FROM activity_log%s ORDER BY timestamp DESC LIMIT ? OFFSET ?", where)
	if err := s.DB.Select(&logs, s.DB.Rebind(q), queryArgs...); err != nil {
		return nil, 0, err
	}

	return logs, total, nil
}

func (s *Store) ClearActivity() (int64, error) {
	var count int64
	if err := s.DB.Get(&count, "SELECT COUNT(*) FROM activity_log"); err != nil {
		return 0, err
	}
	if _, err := s.DB.Exec("DELETE FROM activity_log"); err != nil {
		return 0, err
	}
	return count, nil
}

// --- Users ---

func (s *Store) ListUsers() ([]map[string]any, error) {
	rows, err := s.DB.Queryx(`
		SELECT u.*, COUNT(wh.id) as total_plays, MAX(wh.watched_at) as last_watched
		FROM users u
		LEFT JOIN watch_history wh ON u.id = wh.user_id
		GROUP BY u.id
		ORDER BY LOWER(u.username), u.id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []map[string]any
	for rows.Next() {
		m := make(map[string]any)
		if err := rows.MapScan(m); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, nil
}

// --- Arr Instances ---

func (s *Store) ListArrInstances(kind string) ([]models.ArrInstance, error) {
	var instances []models.ArrInstance
	if kind != "" {
		err := s.DB.Select(&instances,
			s.DB.Rebind("SELECT * FROM arr_instances WHERE kind = ? ORDER BY is_default DESC, id"),
			kind,
		)
		return instances, err
	}
	err := s.DB.Select(&instances, "SELECT * FROM arr_instances ORDER BY kind, is_default DESC, id")
	return instances, err
}

func (s *Store) GetArrInstance(id int64) (*models.ArrInstance, error) {
	var inst models.ArrInstance
	err := s.DB.Get(&inst, s.DB.Rebind("SELECT * FROM arr_instances WHERE id = ?"), id)
	if err != nil {
		return nil, err
	}
	return &inst, nil
}

func (s *Store) DefaultArrInstance(kind string) (*models.ArrInstance, error) {
	var inst models.ArrInstance
	err := s.DB.Get(&inst, s.DB.Rebind("SELECT * FROM arr_instances WHERE kind = ? AND is_default = 1"), kind)
	if err != nil {
		err = s.DB.Get(&inst, s.DB.Rebind("SELECT * FROM arr_instances WHERE kind = ? ORDER BY id LIMIT 1"), kind)
		if err != nil {
			return nil, err
		}
	}
	return &inst, nil
}

func (s *Store) CreateArrInstance(kind, name, url, apiKey, publicURL string, isDefault bool) (int64, error) {
	tx, err := s.DB.Beginx()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	if isDefault {
		tx.Exec(s.DB.Rebind("UPDATE arr_instances SET is_default = 0 WHERE kind = ? AND is_default = 1"), kind)
	}

	res, err := tx.Exec(
		s.DB.Rebind(`INSERT INTO arr_instances (kind, name, url, api_key, public_url, is_default) VALUES (?, ?, ?, ?, ?, ?)`),
		kind, name, url, apiKey, publicURL, isDefault,
	)
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()

	if !isDefault {
		var hasDefault int
		tx.Get(&hasDefault, s.DB.Rebind("SELECT COUNT(*) FROM arr_instances WHERE kind = ? AND is_default = 1"), kind)
		if hasDefault == 0 {
			tx.Exec(s.DB.Rebind("UPDATE arr_instances SET is_default = 1 WHERE id = ?"), id)
		}
	}

	return id, tx.Commit()
}

func (s *Store) UpdateArrInstance(id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}

	allowed := map[string]bool{"name": true, "url": true, "api_key": true, "public_url": true, "is_default": true}

	if isDefault, ok := fields["is_default"]; ok {
		if isDefault == true {
			var kind string
			s.DB.Get(&kind, s.DB.Rebind("SELECT kind FROM arr_instances WHERE id = ?"), id)
			s.DB.Exec(s.DB.Rebind("UPDATE arr_instances SET is_default = 0 WHERE kind = ? AND is_default = 1 AND id != ?"), kind, id)
			fields["is_default"] = 1
		} else {
			fields["is_default"] = 0
		}
	}

	sets := []string{}
	args := []any{}
	for k, v := range fields {
		if !allowed[k] {
			continue
		}
		sets = append(sets, k+" = ?")
		args = append(args, v)
	}
	if len(sets) == 0 {
		return nil
	}

	args = append(args, id)
	q := fmt.Sprintf("UPDATE arr_instances SET %s WHERE id = ?", joinStr(sets, ", "))
	_, err := s.DB.Exec(s.DB.Rebind(q), args...)
	return err
}

func (s *Store) DeleteArrInstance(id int64) error {
	var inst models.ArrInstance
	if err := s.DB.Get(&inst, s.DB.Rebind("SELECT * FROM arr_instances WHERE id = ?"), id); err != nil {
		return err
	}

	if _, err := s.DB.Exec(s.DB.Rebind("DELETE FROM arr_instances WHERE id = ?"), id); err != nil {
		return err
	}

	if inst.IsDefault {
		s.DB.Exec(s.DB.Rebind("UPDATE arr_instances SET is_default = 1 WHERE kind = ? ORDER BY id LIMIT 1"), inst.Kind)
	}
	return nil
}

func (s *Store) ResolveArrInstance(instanceID *int64, kind string) (*models.ArrInstance, error) {
	if instanceID != nil {
		inst, err := s.GetArrInstance(*instanceID)
		if err == nil && inst.Kind == kind {
			return inst, nil
		}
	}
	return s.DefaultArrInstance(kind)
}

func joinStr(parts []string, sep string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += sep
		}
		result += p
	}
	return result
}
