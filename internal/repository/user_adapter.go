package repository

import (
	"context"
	"fmt"
	q "github.com/core-go/cassandra"
	"github.com/core-go/cassandra/template"
	"github.com/core-go/search/convert"
	"github.com/gocql/gocql"
	"reflect"
	"strings"

	. "go-service/internal/model"
)

type UserAdapter struct {
	Cluster       *gocql.ClusterConfig
	ModelType     reflect.Type
	JsonColumnMap map[string]string
	Keys          []string
	FieldsIndex   map[string]int
	Fields        string
	templates     map[string]*template.Template
}

func NewUserRepository(db *gocql.ClusterConfig, templates map[string]*template.Template) (*UserAdapter, error) {
	userType := reflect.TypeOf(User{})
	fieldsIndex, _, jsonColumnMap, keys, _, fields, err := q.Init(userType)
	if err != nil {
		return nil, err
	}
	return &UserAdapter{Cluster: db, ModelType: userType, JsonColumnMap: jsonColumnMap, Keys: keys, Fields: fields, FieldsIndex: fieldsIndex, templates: templates}, nil
}

func (m *UserAdapter) All(ctx context.Context) ([]User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	query := "select id, username, email, phone, date_of_birth from users"
	rows := session.Query(query).Iter()
	var users []User
	var user User
	for rows.Scan(&user.Id, &user.Username, &user.Phone, &user.Email, &user.DateOfBirth) {
		users = append(users, user)
	}
	return users, nil
}

func (m *UserAdapter) Load(ctx context.Context, id string) (*User, error) {
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	var user User
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

func (m *UserAdapter) Create(ctx context.Context, user *User) (int64, error) {
	session, err := m.Cluster.CreateSession()
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

func (m *UserAdapter) Update(ctx context.Context, user *User) (int64, error) {
	session, err := m.Cluster.CreateSession()
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

func (m *UserAdapter) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	colMap := q.JSONToColumns(user, m.JsonColumnMap)
	query, args := q.BuildToPatchWithVersion("users", colMap, m.Keys, "")
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return 0, err
	}
	err = session.Query(query, args...).Exec()
	if err != nil {
		return -1, err
	}
	return 1, nil
}

func (m *UserAdapter) Delete(ctx context.Context, id string) (int64, error) {
	session, err := m.Cluster.CreateSession()
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

func (m *UserAdapter) Search(ctx context.Context, filter *UserFilter) ([]User, string, error) {
	var users []User
	if filter.Limit <= 0 {
		return users, "", nil
	}
	ftr := convert.ToMapWithFields(filter, m.Fields, &m.ModelType)
	query, params := template.Build(ftr, *m.templates["user"])
	session, err := m.Cluster.CreateSession()
	if err != nil {
		return users, "", err
	}
	defer session.Close()
	nextPageToken, err := q.QueryWithPage(session, m.FieldsIndex, &users, filter.Limit, filter.Next, query, params...)
	return users, nextPageToken, err
}
