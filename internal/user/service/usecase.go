package service

import (
	"context"

	"go-service/internal/user/model"
	"go-service/internal/user/repository"
)

func NewUserService(repository repository.UserRepository) *UserUsecase {
	return &UserUsecase{repository: repository}
}

type UserUsecase struct {
	repository repository.UserRepository
}

func (s *UserUsecase) All(ctx context.Context) ([]model.User, error) {
	return s.repository.All(ctx)
}
func (s *UserUsecase) Load(ctx context.Context, id string) (*model.User, error) {
	return s.repository.Load(ctx, id)
}
func (s *UserUsecase) Create(ctx context.Context, user *model.User) (int64, error) {
	return s.repository.Create(ctx, user)
}
func (s *UserUsecase) Update(ctx context.Context, user *model.User) (int64, error) {
	return s.repository.Update(ctx, user)
}
func (s *UserUsecase) Patch(ctx context.Context, user map[string]interface{}) (int64, error) {
	return s.repository.Patch(ctx, user)
}
func (s *UserUsecase) Delete(ctx context.Context, id string) (int64, error) {
	return s.repository.Delete(ctx, id)
}
