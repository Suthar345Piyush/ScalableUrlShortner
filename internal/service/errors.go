// one single error file, from where service layer connects and returns errors

package service

import "errors"

var (
	ErrNotFound   = errors.New("service: short code not found")
	ErrExpired    = errors.New("service: short URL has expired")
	ErrInvalidURL = errors.New("service: invalid URL - must be http or https with a host")
)
