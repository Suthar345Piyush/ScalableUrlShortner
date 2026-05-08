// fiber http handler, which uses fasthttp under the hood
// no business logic here, it just

// parse the input -> call the service -> write the response

package handler

import (
	"github.com/Suthar345Piyush/internal/events"
	"github.com/Suthar345Piyush/internal/service"
	"go.uber.org/zap"
)

// handler struct (service -> service(url service), producer -> events and logs -> zap)

type Handler struct {
	svc      service.URLService
	producer events.Producer
	log      *zap.Logger
}

// new handler constructor

func New(svc service.URLService, producer events.Producer, log *zap.Logger) *Handler {

	return &Handler{svc: svc, producer: producer, log: log}
}
