package playlistimpl

import (
	"context"
	"database/sql"

	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/services/playlist"
	"github.com/grafana/grafana/pkg/services/sqlstore"
	"github.com/grafana/grafana/pkg/services/sqlstore/db"
	"github.com/grafana/grafana/pkg/util"
	"github.com/jmoiron/sqlx"
)

type store interface {
	Insert(context.Context, *playlist.CreatePlaylistCommand) (*playlist.Playlist, error)
	Delete(context.Context, *playlist.DeletePlaylistCommand) error
	Get(context.Context, *playlist.GetPlaylistByUidQuery) (*playlist.Playlist, error)
	GetItems(context.Context, *playlist.GetPlaylistItemsByUidQuery) ([]playlist.PlaylistItem, error)
	List(context.Context, *playlist.GetPlaylistsQuery) (playlist.Playlists, error)
	Update(context.Context, *playlist.UpdatePlaylistCommand) (*playlist.PlaylistDTO, error)
}

type sqlStore struct {
	db     db.DB
	sqlxdb *sqlx.DB
}

func (s *sqlStore) Insert(ctx context.Context, cmd *playlist.CreatePlaylistCommand) (*playlist.Playlist, error) {
	p := playlist.Playlist{}
	// here we are actually doing transaction db session, since there is one session for update 2 tables
	var err error
	if s.sqlxdb == nil {
		err = s.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
			uid, err := generateAndValidateNewPlaylistUid(sess, cmd.OrgId)
			if err != nil {
				return err
			}

			p = playlist.Playlist{
				Name:     cmd.Name,
				Interval: cmd.Interval,
				OrgId:    cmd.OrgId,
				UID:      uid,
			}

			_, err = sess.Insert(&p)
			if err != nil {
				return err
			}

			playlistItems := make([]playlist.PlaylistItem, 0)
			for _, item := range cmd.Items {
				playlistItems = append(playlistItems, playlist.PlaylistItem{
					PlaylistId: p.Id,
					Type:       item.Type,
					Value:      item.Value,
					Order:      item.Order,
					Title:      item.Title,
				})
			}

			_, err = sess.Insert(&playlistItems)

			return err
		})
	} else {
		uid, err := newGenerateAndValidateNewPlaylistUid(ctx, s.sqlxdb, cmd.OrgId)
		if err != nil {
			return nil, err
		}
		p = playlist.Playlist{
			Name:     cmd.Name,
			Interval: cmd.Interval,
			OrgId:    cmd.OrgId,
			UID:      uid,
		}

		tx, err := s.sqlxdb.Beginx()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		if _, err = tx.NamedExecContext(ctx, s.sqlxdb.Rebind("INSERT INTO playlist (name, interval, org_id, uid) VALUES (:name, :interval, :org_id, :uid)"), &p); err != nil {
			return nil, err
		}

		playlistItems := make([]playlist.PlaylistItem, 0)
		for _, item := range cmd.Items {
			playlistItems = append(playlistItems, playlist.PlaylistItem{
				PlaylistId: p.Id,
				Type:       item.Type,
				Value:      item.Value,
				Order:      item.Order,
				Title:      item.Title,
			})
		}
		_, err = tx.NamedExecContext(ctx, s.sqlxdb.Rebind(`INSERT INTO playlist_item (playlist_id, type, value, title, order)
        VALUES (:playlist_id, :type, :value, :title, :order)`), playlistItems)
		if err != nil {
			return nil, err
		}

		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}

	return &p, err
}

func (s *sqlStore) Update(ctx context.Context, cmd *playlist.UpdatePlaylistCommand) (*playlist.PlaylistDTO, error) {
	dto := playlist.PlaylistDTO{}
	err := s.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		p := playlist.Playlist{
			UID:      cmd.UID,
			OrgId:    cmd.OrgId,
			Name:     cmd.Name,
			Interval: cmd.Interval,
		}

		existingPlaylist := playlist.Playlist{UID: cmd.UID, OrgId: cmd.OrgId}
		_, err := sess.Get(&existingPlaylist)
		if err != nil {
			return err
		}
		p.Id = existingPlaylist.Id

		dto = playlist.PlaylistDTO{
			Id:       p.Id,
			UID:      p.UID,
			OrgId:    p.OrgId,
			Name:     p.Name,
			Interval: p.Interval,
		}

		_, err = sess.Where("id=?", p.Id).Cols("name", "interval").Update(&p)
		if err != nil {
			return err
		}

		rawSQL := "DELETE FROM playlist_item WHERE playlist_id = ?"
		_, err = sess.Exec(rawSQL, p.Id)

		if err != nil {
			return err
		}

		playlistItems := make([]models.PlaylistItem, 0)

		for index, item := range cmd.Items {
			playlistItems = append(playlistItems, models.PlaylistItem{
				PlaylistId: p.Id,
				Type:       item.Type,
				Value:      item.Value,
				Order:      index + 1,
				Title:      item.Title,
			})
		}

		_, err = sess.Insert(&playlistItems)
		return err
	})
	return &dto, err
}

func (s *sqlStore) Get(ctx context.Context, query *playlist.GetPlaylistByUidQuery) (*playlist.Playlist, error) {
	if query.UID == "" || query.OrgId == 0 {
		return nil, playlist.ErrCommandValidationFailed
	}

	p := playlist.Playlist{}

	var err error
	if s.sqlxdb == nil {
		err = s.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
			p = playlist.Playlist{UID: query.UID, OrgId: query.OrgId}
			exists, err := sess.Get(&p)
			if !exists {
				return playlist.ErrPlaylistNotFound
			}
			return err
		})
	} else {
		err = s.sqlxdb.GetContext(ctx, &p, s.sqlxdb.Rebind("SELECT * FROM playlist WHERE uid=? AND org_id=?"), query.UID, query.OrgId)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, playlist.ErrPlaylistNotFound
			}
			return nil, err
		}
	}
	return &p, err
}

func (s *sqlStore) Delete(ctx context.Context, cmd *playlist.DeletePlaylistCommand) error {
	if cmd.UID == "" || cmd.OrgId == 0 {
		return playlist.ErrCommandValidationFailed
	}

	var err error
	if s.sqlxdb == nil {
		err = s.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
			playlist := playlist.Playlist{UID: cmd.UID, OrgId: cmd.OrgId}
			_, err := sess.Get(&playlist)
			if err != nil {
				return err
			}

			var rawPlaylistSQL = "DELETE FROM playlist WHERE uid = ? and org_id = ?"
			_, err = sess.Exec(rawPlaylistSQL, cmd.UID, cmd.OrgId)
			if err != nil {
				return err
			}

			var rawItemSQL = "DELETE FROM playlist_item WHERE playlist_id = ?"
			_, err = sess.Exec(rawItemSQL, playlist.Id)

			return err
		})
	} else {
		tx, err := s.sqlxdb.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		p := playlist.Playlist{}
		if err = s.sqlxdb.GetContext(ctx, &p, s.sqlxdb.Rebind("SELECT * FROM playlist WHERE uid=? AND org_id=?"), cmd.UID, cmd.OrgId); err != nil {
			return err
		}

		if _, err = tx.ExecContext(ctx, s.sqlxdb.Rebind("DELETE FROM playlist WHERE uid = ? and org_id = ?"), cmd.UID, cmd.OrgId); err != nil {
			return err
		}

		if _, err = tx.ExecContext(ctx, s.sqlxdb.Rebind("DELETE FROM playlist_item WHERE playlist_id = ?"), p.Id); err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return err
}

func (s *sqlStore) List(ctx context.Context, query *playlist.GetPlaylistsQuery) (playlist.Playlists, error) {
	playlists := make(playlist.Playlists, 0)
	if query.OrgId == 0 {
		return playlists, playlist.ErrCommandValidationFailed
	}

	var err error
	if s.sqlxdb == nil {
		err = s.db.WithDbSession(ctx, func(dbSess *sqlstore.DBSession) error {
			sess := dbSess.Limit(query.Limit)

			if query.Name != "" {
				sess.Where("name LIKE ?", "%"+query.Name+"%")
			}

			sess.Where("org_id = ?", query.OrgId)
			err := sess.Find(&playlists)

			return err
		})
	} else {
		if query.Name == "" {
			err = s.sqlxdb.SelectContext(
				ctx, &playlists, s.sqlxdb.Rebind("SELECT * FROM playlist WHERE org_id = ? LIMIT ?"), query.OrgId, query.Limit)
		} else {
			err = s.sqlxdb.SelectContext(
				ctx, &playlists, s.sqlxdb.Rebind("SELECT * FROM playlist WHERE org_id = ? AND name LIKE ? LIMIT ?"), query.OrgId, "%"+query.Name+"%", query.Limit)
		}
	}
	return playlists, err
}

func (s *sqlStore) GetItems(ctx context.Context, query *playlist.GetPlaylistItemsByUidQuery) ([]playlist.PlaylistItem, error) {
	var playlistItems = make([]playlist.PlaylistItem, 0)
	if query.PlaylistUID == "" || query.OrgId == 0 {
		return playlistItems, models.ErrCommandValidationFailed
	}

	var err error
	if s.sqlxdb == nil {
		err = s.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
			// getQuery the playlist Id
			getQuery := &playlist.GetPlaylistByUidQuery{UID: query.PlaylistUID, OrgId: query.OrgId}
			p, err := s.Get(ctx, getQuery)
			if err != nil {
				return err
			}
			err = sess.Where("playlist_id=?", p.Id).Find(&playlistItems)
			return err
		})
	} else {
		var p = playlist.Playlist{}
		err = s.sqlxdb.GetContext(ctx, &p, s.sqlxdb.Rebind("SELECT * FROM playlist WHERE uid=? AND org_id=?"), query.PlaylistUID, query.OrgId)
		if err != nil {
			return playlistItems, err
		}
		err = s.sqlxdb.SelectContext(ctx, &playlistItems, s.sqlxdb.Rebind("SELECT * FROM playlist_item WHERE playlist_id=?"), p.Id)
	}
	return playlistItems, err
}

// generateAndValidateNewPlaylistUid generates a playlistUID and verifies that
// the uid isn't already in use. This is deliberately overly cautious, since users
// can also specify playlist uids during provisioning.
func generateAndValidateNewPlaylistUid(sess *sqlstore.DBSession, orgId int64) (string, error) {
	for i := 0; i < 3; i++ {
		uid := generateNewUid()

		playlist := models.Playlist{OrgId: orgId, UID: uid}
		exists, err := sess.Get(&playlist)
		if err != nil {
			return "", err
		}

		if !exists {
			return uid, nil
		}
	}

	return "", models.ErrPlaylistFailedGenerateUniqueUid
}

func newGenerateAndValidateNewPlaylistUid(ctx context.Context, db *sqlx.DB, orgId int64) (string, error) {
	for i := 0; i < 3; i++ {
		uid := generateNewUid()
		p := models.Playlist{}
		err := db.GetContext(ctx, &p, db.Rebind("SELECT * FROM playlist WHERE uid=? AND org_id=?"), uid, orgId)
		if err != nil {
			if err == sql.ErrNoRows {
				return uid, nil
			}
			return "", err
		}
	}

	return "", models.ErrPlaylistFailedGenerateUniqueUid
}

var generateNewUid func() string = util.GenerateShortUID
