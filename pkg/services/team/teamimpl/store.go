package teamimpl

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/grafana/pkg/models"
	ac "github.com/grafana/grafana/pkg/services/accesscontrol"
	"github.com/grafana/grafana/pkg/services/sqlstore"
	"github.com/grafana/grafana/pkg/services/sqlstore/db"
	"github.com/grafana/grafana/pkg/services/sqlstore/migrator"
	"github.com/grafana/grafana/pkg/services/team"
	"github.com/grafana/grafana/pkg/setting"
)

type store interface {
	// oprations on team
	Insert(ctx context.Context, team *team.Team) error
	Update(ctx context.Context, cmd *team.Team) error
	Delete(ctx context.Context, cmd *team.DeleteTeamCommand) error
	List(ctx context.Context, query *team.SearchTeamsQuery) (*team.SearchTeamQueryResult, error)
	ListByUser(ctx context.Context, query *team.GetTeamsByUserQuery) (*team.GetTeamsByUserQueryResult, error)
	GetById(ctx context.Context, query *team.GetTeamByIdQuery) error
	GetByName(ctx context.Context, orgID int64, name string, userID int64) (bool, error)
	// operations on team member
	UpdateTeamMember(ctx context.Context, cmd *models.UpdateTeamMemberCommand) error
	RemoveTeamMember(ctx context.Context, cmd *models.RemoveTeamMemberCommand) error
	GetTeamMembers(ctx context.Context, cmd *models.GetTeamMembersQuery) error
	GetUserTeamMemberships(ctx context.Context, orgID, userID int64, external bool) ([]*models.TeamMemberDTO, error)
}

type sqlStore struct {
	db      db.DB
	dialect migrator.Dialect
	Cfg     *setting.Cfg
}

func (ss *sqlStore) Insert(ctx context.Context, team *team.Team) error {
	return ss.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		_, err := sess.Insert(&team)
		return err
	})
}

func (ss *sqlStore) Update(ctx context.Context, cmd *team.Team) error {
	return ss.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		sess.MustCols("email")
		affectedRows, err := sess.ID(cmd.ID).Update(cmd)

		if err != nil {
			return err
		}

		if affectedRows == 0 {
			return models.ErrTeamNotFound
		}

		return nil
	})
}

// DeleteTeam will delete a team, its member and any permissions connected to the team
func (ss *sqlStore) Delete(ctx context.Context, cmd *team.DeleteTeamCommand) error {
	return ss.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		if _, err := teamExists(cmd.OrgID, cmd.ID, sess); err != nil {
			return err
		}

		deletes := []string{
			"DELETE FROM team_member WHERE org_id=? and team_id = ?",
			"DELETE FROM team WHERE org_id=? and id = ?",
			"DELETE FROM dashboard_acl WHERE org_id=? and team_id = ?",
			"DELETE FROM team_role WHERE org_id=? and team_id = ?",
		}

		for _, sql := range deletes {
			_, err := sess.Exec(sql, cmd.OrgID, cmd.ID)
			if err != nil {
				return err
			}
		}

		_, err := sess.Exec("DELETE FROM permission WHERE scope=?", ac.Scope("teams", "id", fmt.Sprint(cmd.Id)))

		return err
	})
}

func (ss *sqlStore) List(ctx context.Context, query *team.SearchTeamsQuery) (*team.SearchTeamQueryResult, error) {
	result := team.SearchTeamQueryResult{
		Teams: make([]*team.TeamDTO, 0),
	}

	err := ss.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
		queryWithWildcards := "%" + query.Query + "%"

		var sql bytes.Buffer
		params := make([]interface{}, 0)

		filteredUsers := getFilteredUsers(query.SignedInUser, query.HiddenUsers)
		for _, user := range filteredUsers {
			params = append(params, user)
		}

		if query.UserIdFilter == models.FilterIgnoreUser {
			sql.WriteString(getTeamSelectSQLBase(filteredUsers, ss.dialect))
		} else {
			sql.WriteString(getTeamSelectWithPermissionsSQLBase(filteredUsers, ss.dialect))
			params = append(params, query.UserIdFilter)
		}

		sql.WriteString(` WHERE team.org_id = ?`)
		params = append(params, query.OrgID)

		if query.Query != "" {
			sql.WriteString(` and team.name ` + ss.dialect.LikeStr() + ` ?`)
			params = append(params, queryWithWildcards)
		}

		if query.Name != "" {
			sql.WriteString(` and team.name = ?`)
			params = append(params, query.Name)
		}

		var (
			acFilter ac.SQLFilter
			err      error
		)
		if query.AcEnabled {
			acFilter, err = ac.Filter(query.SignedInUser, "team.id", "teams:id:", ac.ActionTeamsRead)
			if err != nil {
				return err
			}
			sql.WriteString(` and` + acFilter.Where)
			params = append(params, acFilter.Args...)
		}

		sql.WriteString(` order by team.name asc`)

		if query.Limit != 0 {
			offset := query.Limit * (query.Page - 1)
			sql.WriteString(ss.dialect.LimitOffset(int64(query.Limit), int64(offset)))
		}

		if err := sess.SQL(sql.String(), params...).Find(&result.Teams); err != nil {
			return err
		}

		team := models.Team{}
		countSess := sess.Table("team")
		countSess.Where("team.org_id=?", query.OrgID)

		if query.Query != "" {
			countSess.Where(`name `+ss.dialect.LikeStr()+` ?`, queryWithWildcards)
		}

		if query.Name != "" {
			countSess.Where("name=?", query.Name)
		}

		// If we're not retrieving all results, then only search for teams that this user has access to
		if query.UserIdFilter != models.FilterIgnoreUser {
			countSess.
				Where(`
			team.id IN (
				SELECT
				team_id
				FROM team_member
				WHERE team_member.user_id = ?
			)`, query.UserIdFilter)
		}

		// Only count teams user can see
		if query.AcEnabled {
			countSess.Where(acFilter.Where, acFilter.Args...)
		}

		count, err := countSess.Count(&team)
		result.TotalCount = count

		return err
	})
	return &result, err
}

func getFilteredUsers(signedInUser *models.SignedInUser, hiddenUsers map[string]struct{}) []string {
	filteredUsers := make([]string, 0, len(hiddenUsers))
	if signedInUser == nil || signedInUser.IsGrafanaAdmin {
		return filteredUsers
	}

	for u := range hiddenUsers {
		if u == signedInUser.Login {
			continue
		}
		filteredUsers = append(filteredUsers, u)
	}

	return filteredUsers
}

func getTeamMemberCount(filteredUsers []string, dialect migrator.Dialect) string {
	if len(filteredUsers) > 0 {
		return `(SELECT COUNT(*) FROM team_member
			INNER JOIN ` + dialect.Quote("user") + ` ON team_member.user_id = ` + dialect.Quote("user") + `.id
			WHERE team_member.team_id = team.id AND ` + dialect.Quote("user") + `.login NOT IN (?` +
			strings.Repeat(",?", len(filteredUsers)-1) + ")" +
			`) AS member_count `
	}

	return "(SELECT COUNT(*) FROM team_member WHERE team_member.team_id = team.id) AS member_count "
}

func getTeamSelectSQLBase(filteredUsers []string, dialect migrator.Dialect) string {
	return `SELECT
		team.id as id,
		team.org_id,
		team.name as name,
		team.email as email, ` +
		getTeamMemberCount(filteredUsers, dialect) +
		` FROM team as team `
}

func getTeamSelectWithPermissionsSQLBase(filteredUsers []string, dialect migrator.Dialect) string {
	return `SELECT
		team.id AS id,
		team.org_id,
		team.name AS name,
		team.email AS email,
		team_member.permission, ` +
		getTeamMemberCount(filteredUsers, dialect) +
		` FROM team AS team
		INNER JOIN team_member ON team.id = team_member.team_id AND team_member.user_id = ? `
}

func teamExists(orgID int64, teamID int64, sess *sqlstore.DBSession) (bool, error) {
	if res, err := sess.Query("SELECT 1 from team WHERE org_id=? and id=?", orgID, teamID); err != nil {
		return false, err
	} else if len(res) != 1 {
		return false, models.ErrTeamNotFound
	}

	return true, nil
}

func (ss *sqlStore) GetByName(ctx context.Context, orgId int64, name string, existingId int64) (bool, error) {
	var team models.Team
	var exists bool
	var err error
	err = ss.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
		exists, err = sess.Where("org_id=? and name=?", orgId, name).Get(&team)
		return err
	})

	if err != nil {
		return false, err
	}

	if exists && existingId != team.Id {
		return true, nil
	}

	return false, nil
}

func (ss *sqlStore) GetById(ctx context.Context, query *team.GetTeamByIdQuery) error {
	return ss.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
		var sql bytes.Buffer
		params := make([]interface{}, 0)

		filteredUsers := getFilteredUsers(query.SignedInUser, query.HiddenUsers)
		sql.WriteString(getTeamSelectSQLBase(filteredUsers, ss.dialect))
		for _, user := range filteredUsers {
			params = append(params, user)
		}

		if query.UserIdFilter != models.FilterIgnoreUser {
			sql.WriteString(` INNER JOIN team_member ON team.id = team_member.team_id AND team_member.user_id = ?`)
			params = append(params, query.UserIdFilter)
		}

		sql.WriteString(` WHERE team.org_id = ? and team.id = ?`)
		params = append(params, query.OrgID, query.ID)

		var team team.TeamDTO
		exists, err := sess.SQL(sql.String(), params...).Get(&team)

		if err != nil {
			return err
		}

		if !exists {
			return models.ErrTeamNotFound
		}

		query.Result = &team
		return nil
	})
}

// GetTeamsByUser is used by the Guardian when checking a users' permissions
func (ss *sqlStore) ListByUser(ctx context.Context, query *team.GetTeamsByUserQuery) (*team.GetTeamsByUserQueryResult, error) {
	result := team.GetTeamsByUserQueryResult{Result: make([]*team.TeamDTO, 0)}
	err := ss.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
		var sql bytes.Buffer
		sql.WriteString(getTeamSelectSQLBase([]string{}, ss.dialect))
		sql.WriteString(` INNER JOIN team_member on team.id = team_member.team_id`)
		sql.WriteString(` WHERE team.org_id = ? and team_member.user_id = ?`)

		err := sess.SQL(sql.String(), query.OrgID, query.UserID).Find(&result.Result)
		return err
	})
	return &result, err
}

// AddTeamMember adds a user to a team
func (ss *sqlStore) AddTeamMember(userID, orgID, teamID int64, isExternal bool, permission models.PermissionType) error {
	return ss.db.WithTransactionalDbSession(context.Background(), func(sess *sqlstore.DBSession) error {
		if isMember, err := isTeamMember(sess, orgID, teamID, userID); err != nil {
			return err
		} else if isMember {
			return models.ErrTeamMemberAlreadyAdded
		}

		return addTeamMember(sess, orgID, teamID, userID, isExternal, permission)
	})
}

func getTeamMember(sess *sqlstore.DBSession, orgId int64, teamId int64, userId int64) (models.TeamMember, error) {
	rawSQL := `SELECT * FROM team_member WHERE org_id=? and team_id=? and user_id=?`
	var member models.TeamMember
	exists, err := sess.SQL(rawSQL, orgId, teamId, userId).Get(&member)

	if err != nil {
		return member, err
	}
	if !exists {
		return member, models.ErrTeamMemberNotFound
	}

	return member, nil
}

// UpdateTeamMember updates a team member
func (ss *sqlStore) UpdateTeamMember(ctx context.Context, cmd *models.UpdateTeamMemberCommand) error {
	return ss.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		return updateTeamMember(sess, cmd.OrgId, cmd.TeamId, cmd.UserId, cmd.Permission)
	})
}

func (ss *sqlStore) IsTeamMember(orgId int64, teamId int64, userId int64) (bool, error) {
	var isMember bool

	err := ss.db.WithTransactionalDbSession(context.Background(), func(sess *sqlstore.DBSession) error {
		var err error
		isMember, err = isTeamMember(sess, orgId, teamId, userId)
		return err
	})

	return isMember, err
}

func isTeamMember(sess *sqlstore.DBSession, orgId int64, teamId int64, userId int64) (bool, error) {
	if res, err := sess.Query("SELECT 1 FROM team_member WHERE org_id=? and team_id=? and user_id=?", orgId, teamId, userId); err != nil {
		return false, err
	} else if len(res) != 1 {
		return false, nil
	}

	return true, nil
}

// AddOrUpdateTeamMemberHook is called from team resource permission service
// it adds user to a team or updates user permissions in a team within the given transaction session
func AddOrUpdateTeamMemberHook(sess *sqlstore.DBSession, userID, orgID, teamID int64, isExternal bool, permission models.PermissionType) error {
	isMember, err := isTeamMember(sess, orgID, teamID, userID)
	if err != nil {
		return err
	}

	if isMember {
		err = updateTeamMember(sess, orgID, teamID, userID, permission)
	} else {
		err = addTeamMember(sess, orgID, teamID, userID, isExternal, permission)
	}

	return err
}

func addTeamMember(sess *sqlstore.DBSession, orgID, teamID, userID int64, isExternal bool, permission models.PermissionType) error {
	if _, err := teamExists(orgID, teamID, sess); err != nil {
		return err
	}

	entity := models.TeamMember{
		OrgId:      orgID,
		TeamId:     teamID,
		UserId:     userID,
		External:   isExternal,
		Created:    time.Now(),
		Updated:    time.Now(),
		Permission: permission,
	}

	_, err := sess.Insert(&entity)
	return err
}

func updateTeamMember(sess *sqlstore.DBSession, orgID, teamID, userID int64, permission models.PermissionType) error {
	member, err := getTeamMember(sess, orgID, teamID, userID)
	if err != nil {
		return err
	}

	if permission != models.PERMISSION_ADMIN {
		permission = 0 // make sure we don't get invalid permission levels in store

		// protect the last team admin
		_, err := isLastAdmin(sess, orgID, teamID, userID)
		if err != nil {
			return err
		}
	}

	member.Permission = permission
	_, err = sess.Cols("permission").Where("org_id=? and team_id=? and user_id=?", orgID, teamID, userID).Update(member)
	return err
}

// RemoveTeamMember removes a member from a team
func (ss *sqlStore) RemoveTeamMember(ctx context.Context, cmd *models.RemoveTeamMemberCommand) error {
	return ss.db.WithTransactionalDbSession(ctx, func(sess *sqlstore.DBSession) error {
		return removeTeamMember(sess, cmd)
	})
}

// RemoveTeamMemberHook is called from team resource permission service
// it removes a member from a team within the given transaction session
func RemoveTeamMemberHook(sess *sqlstore.DBSession, cmd *models.RemoveTeamMemberCommand) error {
	return removeTeamMember(sess, cmd)
}

func removeTeamMember(sess *sqlstore.DBSession, cmd *models.RemoveTeamMemberCommand) error {
	if _, err := teamExists(cmd.OrgId, cmd.TeamId, sess); err != nil {
		return err
	}

	_, err := isLastAdmin(sess, cmd.OrgId, cmd.TeamId, cmd.UserId)
	if err != nil {
		return err
	}

	var rawSQL = "DELETE FROM team_member WHERE org_id=? and team_id=? and user_id=?"
	res, err := sess.Exec(rawSQL, cmd.OrgId, cmd.TeamId, cmd.UserId)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if rows == 0 {
		return models.ErrTeamMemberNotFound
	}

	return err
}

func isLastAdmin(sess *sqlstore.DBSession, orgId int64, teamId int64, userId int64) (bool, error) {
	rawSQL := "SELECT user_id FROM team_member WHERE org_id=? and team_id=? and permission=?"
	userIds := []*int64{}
	err := sess.SQL(rawSQL, orgId, teamId, models.PERMISSION_ADMIN).Find(&userIds)
	if err != nil {
		return false, err
	}

	isAdmin := false
	for _, adminId := range userIds {
		if userId == *adminId {
			isAdmin = true
			break
		}
	}

	if isAdmin && len(userIds) == 1 {
		return true, models.ErrLastTeamAdmin
	}

	return false, err
}

// GetUserTeamMemberships return a list of memberships to teams granted to a user
// If external is specified, only memberships provided by an external auth provider will be listed
// This function doesn't perform any accesscontrol filtering.
func (ss *sqlStore) GetUserTeamMemberships(ctx context.Context, orgID, userID int64, external bool) ([]*models.TeamMemberDTO, error) {
	query := &models.GetTeamMembersQuery{
		OrgId:    orgID,
		UserId:   userID,
		External: external,
		Result:   []*models.TeamMemberDTO{},
	}
	err := ss.getTeamMembers(ctx, query, nil)
	return query.Result, err
}

// GetTeamMembers return a list of members for the specified team filtered based on the user's permissions
func (ss *sqlStore) GetTeamMembers(ctx context.Context, query *models.GetTeamMembersQuery) error {
	acFilter := &ac.SQLFilter{}
	var err error

	// With accesscontrol we filter out users based on the SignedInUser's permissions
	// Note we assume that checking SignedInUser is allowed to see team members for this team has already been performed
	// If the signed in user is not set no member will be returned
	if !ac.IsDisabled(ss.Cfg) {
		sqlID := fmt.Sprintf("%s.%s", ss.dialect.Quote("user"), ss.dialect.Quote("id"))
		*acFilter, err = ac.Filter(query.SignedInUser, sqlID, "users:id:", ac.ActionOrgUsersRead)
		if err != nil {
			return err
		}
	}

	return ss.getTeamMembers(ctx, query, acFilter)
}

// getTeamMembers return a list of members for the specified team
func (ss *sqlStore) getTeamMembers(ctx context.Context, query *models.GetTeamMembersQuery, acUserFilter *ac.SQLFilter) error {
	return ss.db.WithDbSession(ctx, func(dbSess *sqlstore.DBSession) error {
		query.Result = make([]*models.TeamMemberDTO, 0)
		sess := dbSess.Table("team_member")
		sess.Join("INNER", ss.dialect.Quote("user"),
			fmt.Sprintf("team_member.user_id=%s.%s", ss.dialect.Quote("user"), ss.dialect.Quote("id")),
		)

		if acUserFilter != nil {
			sess.Where(acUserFilter.Where, acUserFilter.Args...)
		}

		// Join with only most recent auth module
		authJoinCondition := `(
		SELECT id from user_auth
			WHERE user_auth.user_id = team_member.user_id
			ORDER BY user_auth.created DESC `
		authJoinCondition = "user_auth.id=" + authJoinCondition + ss.dialect.Limit(1) + ")"
		sess.Join("LEFT", "user_auth", authJoinCondition)

		if query.OrgId != 0 {
			sess.Where("team_member.org_id=?", query.OrgId)
		}
		if query.TeamId != 0 {
			sess.Where("team_member.team_id=?", query.TeamId)
		}
		if query.UserId != 0 {
			sess.Where("team_member.user_id=?", query.UserId)
		}
		if query.External {
			sess.Where("team_member.external=?", ss.dialect.BooleanStr(true))
		}
		sess.Cols(
			"team_member.org_id",
			"team_member.team_id",
			"team_member.user_id",
			"user.email",
			"user.name",
			"user.login",
			"team_member.external",
			"team_member.permission",
			"user_auth.auth_module",
		)
		sess.Asc("user.login", "user.email")

		err := sess.Find(&query.Result)
		return err
	})
}

func (ss *sqlStore) IsAdminOfTeams(ctx context.Context, query *models.IsAdminOfTeamsQuery) error {
	return ss.db.WithDbSession(ctx, func(sess *sqlstore.DBSession) error {
		builder := &sqlstore.SQLBuilder{}
		builder.Write("SELECT COUNT(team.id) AS count FROM team INNER JOIN team_member ON team_member.team_id = team.id WHERE team.org_id = ? AND team_member.user_id = ? AND team_member.permission = ?", query.SignedInUser.OrgId, query.SignedInUser.UserId, models.PERMISSION_ADMIN)

		type teamCount struct {
			Count int64
		}

		resp := make([]*teamCount, 0)
		if err := sess.SQL(builder.GetSQLString(), builder.Params...).Find(&resp); err != nil {
			return err
		}

		query.Result = len(resp) > 0 && resp[0].Count > 0

		return nil
	})
}