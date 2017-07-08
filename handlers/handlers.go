package handlers

import (
	"io/ioutil"
	"net/http"
	"strconv"

	"code.cloudfoundry.org/auctioneer"
	"code.cloudfoundry.org/bbs"
	"code.cloudfoundry.org/bbs/controllers"
	"code.cloudfoundry.org/bbs/db"
	"code.cloudfoundry.org/bbs/events"
	"code.cloudfoundry.org/bbs/handlers/middleware"
	"code.cloudfoundry.org/bbs/models"
	"code.cloudfoundry.org/bbs/serviceclient"
	"code.cloudfoundry.org/bbs/taskworkpool"
	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/rep"
	"github.com/gogo/protobuf/proto"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/tedsuo/rata"
)

func New(
	logger,
	accessLogger lager.Logger,
	updateWorkers int,
	convergenceWorkersSize int,
	statsEmitter middleware.Emitter,
	db db.DB,
	desiredHub, actualHub, taskHub events.Hub,
	taskCompletionClient taskworkpool.TaskCompletionClient,
	serviceClient serviceclient.ServiceClient,
	auctioneerClient auctioneer.Client,
	repClientFactory rep.ClientFactory,
	migrationsDone <-chan struct{},
	exitChan chan struct{},
	tracer opentracing.Tracer,
) http.Handler {
	pingHandler := NewPingHandler()
	domainHandler := NewDomainHandler(db, exitChan)
	actualLRPHandler := NewActualLRPHandler(db, exitChan)
	actualLRPController := controllers.NewActualLRPLifecycleController(db, db, db, auctioneerClient, serviceClient, repClientFactory, actualHub)
	actualLRPLifecycleHandler := NewActualLRPLifecycleHandler(actualLRPController, exitChan)
	evacuationHandler := NewEvacuationHandler(db, db, db, actualHub, auctioneerClient, exitChan)
	desiredLRPHandler := NewDesiredLRPHandler(updateWorkers, db, db, desiredHub, actualHub, auctioneerClient, repClientFactory, serviceClient, exitChan)
	taskController := controllers.NewTaskController(db, taskCompletionClient, auctioneerClient, serviceClient, repClientFactory, taskHub)
	taskHandler := NewTaskHandler(taskController, exitChan)
	eventsHandler := NewEventHandler(desiredHub, actualHub)
	taskEventsHandler := NewTaskEventHandler(taskHub)
	cellsHandler := NewCellHandler(serviceClient, exitChan)

	emitter := middleware.NewLatencyEmitterWrapper(statsEmitter)

	actions := rata.Handlers{
		// Ping
		bbs.PingRoute: emitter.RecordLatency(middleware.FromHTTPRequest(tracer, "ping", middleware.LogWrap(logger, accessLogger, pingHandler.Ping))),

		// Domains
		bbs.DomainsRoute:      route(middleware.FromHTTPRequest(tracer, "domains", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, domainHandler.Domains)))),
		bbs.UpsertDomainRoute: route(middleware.FromHTTPRequest(tracer, "upsertDomain", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, domainHandler.Upsert)))),

		// Actual LRPs
		bbs.ActualLRPGroupsRoute:                     route(middleware.FromHTTPRequest(tracer, "actualLRPGroups", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPHandler.ActualLRPGroups)))),
		bbs.ActualLRPGroupsByProcessGuidRoute:        route(middleware.FromHTTPRequest(tracer, "actualLRPGroupsByProcessGuid", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPHandler.ActualLRPGroupsByProcessGuid)))),
		bbs.ActualLRPGroupByProcessGuidAndIndexRoute: route(middleware.FromHTTPRequest(tracer, "actualLRPGroupByProcessGuidAndIndex", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPHandler.ActualLRPGroupByProcessGuidAndIndex)))),

		// Actual LRP Lifecycle
		bbs.ClaimActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "claimActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.ClaimActualLRP)))),
		bbs.StartActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "startActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.StartActualLRP)))),
		bbs.CrashActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "crashActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.CrashActualLRP)))),
		bbs.RetireActualLRPRoute: route(middleware.FromHTTPRequest(tracer, "retireActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.RetireActualLRP)))),
		bbs.FailActualLRPRoute:   route(middleware.FromHTTPRequest(tracer, "failActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.FailActualLRP)))),
		bbs.RemoveActualLRPRoute: route(middleware.FromHTTPRequest(tracer, "removeActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, actualLRPLifecycleHandler.RemoveActualLRP)))),

		// Evacuation
		bbs.RemoveEvacuatingActualLRPRoute: route(middleware.FromHTTPRequest(tracer, "removeEvacuatingActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, evacuationHandler.RemoveEvacuatingActualLRP)))),
		bbs.EvacuateClaimedActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "evacuateClaimedActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, evacuationHandler.EvacuateClaimedActualLRP)))),
		bbs.EvacuateCrashedActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "evacuateCrashedActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, evacuationHandler.EvacuateCrashedActualLRP)))),
		bbs.EvacuateStoppedActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "evacuateStoppedActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, evacuationHandler.EvacuateStoppedActualLRP)))),
		bbs.EvacuateRunningActualLRPRoute:  route(middleware.FromHTTPRequest(tracer, "evacuateRunningActualLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, evacuationHandler.EvacuateRunningActualLRP)))),

		// Desired LRPs
		bbs.DesiredLRPsRoute:               route(middleware.FromHTTPRequest(tracer, "desiredLRPs", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPs)))),
		bbs.DesiredLRPByProcessGuidRoute:   route(middleware.FromHTTPRequest(tracer, "desiredLRPByProcessGuid", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPByProcessGuid)))),
		bbs.DesiredLRPSchedulingInfosRoute: route(middleware.FromHTTPRequest(tracer, "desiredLRPSchedulingInfos", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPSchedulingInfos)))),
		bbs.DesireDesiredLRPRoute:          route(middleware.FromHTTPRequest(tracer, "desireDesiredLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesireDesiredLRP)))),
		bbs.UpdateDesiredLRPRoute:          route(middleware.FromHTTPRequest(tracer, "updateDesiredLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.UpdateDesiredLRP)))),
		bbs.RemoveDesiredLRPRoute:          route(middleware.FromHTTPRequest(tracer, "removeDesiredLRP", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.RemoveDesiredLRP)))),

		bbs.DesiredLRPsRoute_r0:             route(middleware.FromHTTPRequest(tracer, "desiredLRPs_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPs_r0)))),
		bbs.DesiredLRPsRoute_r1:             route(middleware.FromHTTPRequest(tracer, "desiredLRPs_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPs_r1)))),
		bbs.DesiredLRPByProcessGuidRoute_r0: route(middleware.FromHTTPRequest(tracer, "desiredLRPByProcessGuid_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPByProcessGuid_r0)))),
		bbs.DesiredLRPByProcessGuidRoute_r1: route(middleware.FromHTTPRequest(tracer, "desiredLRPByProcessGuid_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesiredLRPByProcessGuid_r1)))),
		bbs.DesireDesiredLRPRoute_r0:        route(middleware.FromHTTPRequest(tracer, "desireDesiredLRP_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesireDesiredLRP_r0)))),
		bbs.DesireDesiredLRPRoute_r1:        route(middleware.FromHTTPRequest(tracer, "desireDesiredLRP_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, desiredLRPHandler.DesireDesiredLRP_r1)))),

		// Tasks
		bbs.TasksRoute:         route(middleware.FromHTTPRequest(tracer, "tasks", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.Tasks)))),
		bbs.TaskByGuidRoute:    route(middleware.FromHTTPRequest(tracer, "taskByGuid", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.TaskByGuid)))),
		bbs.DesireTaskRoute:    route(middleware.FromHTTPRequest(tracer, "desireTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.DesireTask)))),
		bbs.StartTaskRoute:     route(middleware.FromHTTPRequest(tracer, "startTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.StartTask)))),
		bbs.CancelTaskRoute:    route(middleware.FromHTTPRequest(tracer, "cancelTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.CancelTask)))),
		bbs.FailTaskRoute:      route(middleware.FromHTTPRequest(tracer, "failTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.FailTask)))),
		bbs.CompleteTaskRoute:  route(middleware.FromHTTPRequest(tracer, "completeTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.CompleteTask)))),
		bbs.ResolvingTaskRoute: route(middleware.FromHTTPRequest(tracer, "resolvingTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.ResolvingTask)))),
		bbs.DeleteTaskRoute:    route(middleware.FromHTTPRequest(tracer, "deleteTask", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.DeleteTask)))),

		bbs.TasksRoute_r1:      route(middleware.FromHTTPRequest(tracer, "tasks_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.Tasks_r1)))),
		bbs.TasksRoute_r0:      route(middleware.FromHTTPRequest(tracer, "tasks_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.Tasks_r0)))),
		bbs.TaskByGuidRoute_r1: route(middleware.FromHTTPRequest(tracer, "taskByGuid_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.TaskByGuid_r1)))),
		bbs.TaskByGuidRoute_r0: route(middleware.FromHTTPRequest(tracer, "taskByGuid_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.TaskByGuid_r0)))),
		bbs.DesireTaskRoute_r1: route(middleware.FromHTTPRequest(tracer, "desireTask_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.DesireTask_r1)))),
		bbs.DesireTaskRoute_r0: route(middleware.FromHTTPRequest(tracer, "desireTask_r0", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, taskHandler.DesireTask_r0)))),

		// Events
		bbs.EventStreamRoute_r0:     route(middleware.LogWrap(logger, accessLogger, eventsHandler.Subscribe_r0)),
		bbs.TaskEventStreamRoute_r0: route(middleware.LogWrap(logger, accessLogger, taskEventsHandler.Subscribe_r0)),

		// Cells
		bbs.CellsRoute:    route(middleware.FromHTTPRequest(tracer, "cells", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, cellsHandler.Cells)))),
		bbs.CellsRoute_r1: route(middleware.FromHTTPRequest(tracer, "cells_r1", emitter.RecordLatency(middleware.LogWrap(logger, accessLogger, cellsHandler.Cells)))),
	}

	handler, err := rata.NewRouter(bbs.Routes, actions)
	if err != nil {
		panic("unable to create router: " + err.Error())
	}

	return middleware.RequestCountWrapWithCustomEmitter(
		UnavailableWrap(handler,
			migrationsDone,
		),
		statsEmitter,
	)
}

func route(f http.HandlerFunc) http.Handler {
	return f
}

func parseRequest(logger lager.Logger, req *http.Request, request MessageValidator) error {
	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		logger.Error("failed-to-read-body", err)
		return models.ErrUnknownError
	}

	err = request.Unmarshal(data)
	if err != nil {
		logger.Error("failed-to-parse-request-body", err)
		return models.ErrBadRequest
	}

	if err := request.Validate(); err != nil {
		logger.Error("invalid-request", err)
		return models.NewError(models.Error_InvalidRequest, err.Error())
	}

	return nil
}

func exitIfUnrecoverable(logger lager.Logger, exitCh chan<- struct{}, err *models.Error) {
	if err != nil && err.Type == models.Error_Unrecoverable {
		logger.Error("unrecoverable-error", err)
		select {
		case exitCh <- struct{}{}:
		default:
		}
	}
}

func writeResponse(w http.ResponseWriter, message proto.Message) {
	responseBytes, err := proto.Marshal(message)
	if err != nil {
		panic("Unable to encode Proto: " + err.Error())
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(responseBytes)))
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)

	w.Write(responseBytes)
}
