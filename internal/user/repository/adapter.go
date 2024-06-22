package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	q "github.com/core-go/cassandra"
	"github.com/gocql/gocql"

	"go-service/internal/user/model"
)

type UserAdapter struct {
	Cluster       *gocql.ClusterConfig
	Keys          []string
	JsonColumnMap map[string]string
}

func NewUserRepository(db *gocql.ClusterConfig) *UserAdapter {
	userType := reflect.TypeOf(model.User{})
	jsonColumnMap := q.MakeJsonColumnMap(userType)
	keys, _ := q.FindPrimaryKeys(userType)
	return &UserAdapter{Cluster: db, Keys: keys, JsonColumnMap: jsonColumnMap}
}

func (a *UserAdapter) All(ctx context.Context) (*[]model.User, error) {
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	query := "select id, username, email, phone, date_of_birth from users"
	rows := session.Query(query).Iter()
	var users []model.User
	var user model.User
	for rows.Scan(&user.Id, &user.Username, &user.Phone, &user.Email, &user.DateOfBirth) {
		users = append(users, user)
	}
	return &users, nil
}

func (a *UserAdapter) Load(ctx context.Context, id string) (*model.User, error) {
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	var user model.User
	query := "select id, username, email, phone, date_of_birth from users where id = ?"
	err = session.Query(query, id).Scan(&user.Id, &user.Username, &user.Email, &user.Phone, &user.DateOfBirth)
	if err != nil {
		errMsg := err.Error()
		if strings.Compare(fmt.Sprintf(errMsg), "0 row(s) returned") == 0 {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &user, nil
}

func (a *UserAdapter) Create(ctx context.Context, user *model.User) (int64, error) {
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	query := "insert into users (id, username, email, phone, date_of_birth) values (?, ?, ?, ?, ?)"
	err = session.Query(query, user.Id, user.Username, user.Email, user.Phone, user.DateOfBirth).Exec()
	if err != nil {
		return -1, nil
	}
	return 1, nil
}

func (a *UserAdapter) Update(ctx context.Context, user *model.User) (int64, error) {
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	query := "update users set username = ?, email = ?, phone = ?, date_of_birth = ? where id = ?"
	err = session.Query(query, user.Username, user.Email, user.Phone, user.DateOfBirth, user.Id).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (a *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(user, a.JsonColumnMap)
	query, args := q.BuildToPatch("users", colMap, a.Keys)
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	err = session.Query(query, args...).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (a *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	session, err := a.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	query := "delete from users where id = ?"
	er1 := session.Query(query, id).Exec()
	if er1 != nil {
		return -1, er1
	}
	return 1, nil
}
