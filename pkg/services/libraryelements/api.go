package libraryelements

import (
	"errors"
	"net/http"

	"github.com/grafana/grafana/pkg/api/response"
	"github.com/grafana/grafana/pkg/api/routing"
	"github.com/grafana/grafana/pkg/middleware"
	"github.com/grafana/grafana/pkg/models"
	"github.com/grafana/grafana/pkg/services/dashboards"
	"github.com/grafana/grafana/pkg/web"
)

func (l *LibraryElementService) registerAPIEndpoints() {
	l.RouteRegister.Group("/api/library-elements", func(entities routing.RouteRegister) {
		entities.Post("/", middleware.ReqSignedIn, routing.Wrap(l.createHandler))
		entities.Delete("/:uid", middleware.ReqSignedIn, routing.Wrap(l.deleteHandler))
		entities.Get("/", middleware.ReqSignedIn, routing.Wrap(l.getAllHandler))
		entities.Get("/:uid", middleware.ReqSignedIn, routing.Wrap(l.getHandler))
		entities.Get("/:uid/connections/", middleware.ReqSignedIn, routing.Wrap(l.getConnectionsHandler))
		entities.Get("/name/:name", middleware.ReqSignedIn, routing.Wrap(l.getByNameHandler))
		entities.Patch("/:uid", middleware.ReqSignedIn, routing.Wrap(l.patchHandler))
	})
}

// swagger:route POST /library-elements library_elements createLibraryElement
//
// Create library element.
//
// Creates a new library element.
//
// Responses:
// 200: getLibraryElementResponse
// 400: badRequestError
// 401: unauthorisedError
// 403: forbiddenError
// 404: notFoundError
// 500: internalServerError
func (l *LibraryElementService) createHandler(c *models.ReqContext) response.Response {
	cmd := CreateLibraryElementCommand{}
	if err := web.Bind(c.Req, &cmd); err != nil {
		return response.Error(http.StatusBadRequest, "bad request data", err)
	}

	if cmd.FolderUID != nil {
		if *cmd.FolderUID == "" {
			cmd.FolderID = 0
		} else {
			folder, err := l.folderService.GetFolderByUID(c.Req.Context(), c.SignedInUser, c.OrgId, *cmd.FolderUID)
			if err != nil || folder == nil {
				return response.Error(http.StatusBadRequest, "failed to get folder", err)
			}
			cmd.FolderID = folder.Id
		}
	}

	element, err := l.createLibraryElement(c.Req.Context(), c.SignedInUser, cmd)
	if err != nil {
		return toLibraryElementError(err, "Failed to create library element")
	}

	if element.FolderID != 0 {
		folder, err := l.folderService.GetFolderByID(c.Req.Context(), c.SignedInUser, element.FolderID, c.OrgId)
		if err != nil {
			return response.Error(http.StatusInternalServerError, "failed to get folder", err)
		}
		element.FolderUID = folder.Uid
		element.Meta.FolderUID = folder.Uid
		element.Meta.FolderName = folder.Title
	}

	return response.JSON(http.StatusOK, LibraryElementResponse{Result: element})
}

// swagger:route DELETE /library-elements/{library_element_uid} library_elements deleteLibraryElementByUID
//
// Delete library element.
//
// Deletes an existing library element as specified by the UID. This operation cannot be reverted.
// You cannot delete a library element that is connected. This operation cannot be reverted.
//
// Responses:
// 200: okResponse
// 400: badRequestError
// 401: unauthorisedError
// 403: forbiddenError
// 404: notFoundError
// 500: internalServerError
func (l *LibraryElementService) deleteHandler(c *models.ReqContext) response.Response {
	id, err := l.deleteLibraryElement(c.Req.Context(), c.SignedInUser, web.Params(c.Req)[":uid"])
	if err != nil {
		return toLibraryElementError(err, "Failed to delete library element")
	}

	return response.JSON(http.StatusOK, DeleteLibraryElementResponse{
		Message: "Library element deleted",
		ID:      id,
	})
}

// swagger:route GET /library-elements/{library_element_uid} library_elements getLibraryElementByUID
//
// Get library element by UID.
//
// Returns a library element with the given UID.
//
// Responses:
// 200: getLibraryElementResponse
// 401: unauthorisedError
// 404: notFoundError
// 500: internalServerError
func (l *LibraryElementService) getHandler(c *models.ReqContext) response.Response {
	element, err := l.getLibraryElementByUid(c.Req.Context(), c.SignedInUser, web.Params(c.Req)[":uid"])
	if err != nil {
		return toLibraryElementError(err, "Failed to get library element")
	}

	return response.JSON(http.StatusOK, LibraryElementResponse{Result: element})
}

// swagger:route GET /library-elements library_elements getLibraryElements
//
// Get all library elements.
//
// Returns a list of all library elements the authenticated user has permission to view.
// Use the `perPage` query parameter to control the maximum number of library elements returned; the default limit is `100`.
// You can also use the `page` query parameter to fetch library elements from any page other than the first one.
//
// Responses:
// 200: getLibraryElementsResponse
// 401: unauthorisedError
// 500: internalServerError
func (l *LibraryElementService) getAllHandler(c *models.ReqContext) response.Response {
	query := searchLibraryElementsQuery{
		perPage:       c.QueryInt("perPage"),
		page:          c.QueryInt("page"),
		searchString:  c.Query("searchString"),
		sortDirection: c.Query("sortDirection"),
		kind:          c.QueryInt("kind"),
		typeFilter:    c.Query("typeFilter"),
		excludeUID:    c.Query("excludeUid"),
		folderFilter:  c.Query("folderFilter"),
	}
	elementsResult, err := l.getAllLibraryElements(c.Req.Context(), c.SignedInUser, query)
	if err != nil {
		return toLibraryElementError(err, "Failed to get library elements")
	}

	return response.JSON(http.StatusOK, LibraryElementSearchResponse{Result: elementsResult})
}

// swagger:route PATCH /library-elements/{library_element_uid} library_elements updateLibraryElement
//
// Update library element.
//
// Updates an existing library element identified by uid.
//
// Responses:
// 200: getLibraryElementResponse
// 400: badRequestError
// 401: unauthorisedError
// 403: forbiddenError
// 404: notFoundError
// 412: preconditionFailedError
// 500: internalServerError
func (l *LibraryElementService) patchHandler(c *models.ReqContext) response.Response {
	cmd := PatchLibraryElementCommand{}
	if err := web.Bind(c.Req, &cmd); err != nil {
		return response.Error(http.StatusBadRequest, "bad request data", err)
	}

	if cmd.FolderUID != nil {
		if *cmd.FolderUID == "" {
			cmd.FolderID = 0
		} else {
			folder, err := l.folderService.GetFolderByUID(c.Req.Context(), c.SignedInUser, c.OrgId, *cmd.FolderUID)
			if err != nil || folder == nil {
				return response.Error(http.StatusBadRequest, "failed to get folder", err)
			}
			cmd.FolderID = folder.Id
		}
	}

	element, err := l.patchLibraryElement(c.Req.Context(), c.SignedInUser, cmd, web.Params(c.Req)[":uid"])
	if err != nil {
		return toLibraryElementError(err, "Failed to update library element")
	}

	if element.FolderID != 0 {
		folder, err := l.folderService.GetFolderByID(c.Req.Context(), c.SignedInUser, element.FolderID, c.OrgId)
		if err != nil {
			return response.Error(http.StatusInternalServerError, "failed to get folder", err)
		}
		element.FolderUID = folder.Uid
		element.Meta.FolderUID = folder.Uid
		element.Meta.FolderName = folder.Title
	}

	return response.JSON(http.StatusOK, LibraryElementResponse{Result: element})
}

// swagger:route GET /library-elements/{library_element_uid}/connections/ library_elements getLibraryElementConnections
//
// Get library element connections.
//
// Returns a list of connections for a library element based on the UID specified.
//
// Responses:
// 200: getLibraryElementConnectionsResponse
// 401: unauthorisedError
// 404: notFoundError
// 500: internalServerError
func (l *LibraryElementService) getConnectionsHandler(c *models.ReqContext) response.Response {
	connections, err := l.getConnections(c.Req.Context(), c.SignedInUser, web.Params(c.Req)[":uid"])
	if err != nil {
		return toLibraryElementError(err, "Failed to get connections")
	}

	return response.JSON(http.StatusOK, LibraryElementConnectionsResponse{Result: connections})
}

// swagger:route GET /library-elements/name/{library_element_name} library_elements getLibraryElementByName
//
// Get library element by name.
//
// Returns a library element with the given name.
//
// Responses:
// 200: getLibraryElementResponse
// 401: unauthorisedError
// 404: notFoundError
// 500: internalServerError
func (l *LibraryElementService) getByNameHandler(c *models.ReqContext) response.Response {
	elements, err := l.getLibraryElementsByName(c.Req.Context(), c.SignedInUser, web.Params(c.Req)[":name"])
	if err != nil {
		return toLibraryElementError(err, "Failed to get library element")
	}

	return response.JSON(http.StatusOK, LibraryElementArrayResponse{Result: elements})
}

func toLibraryElementError(err error, message string) response.Response {
	if errors.Is(err, errLibraryElementAlreadyExists) {
		return response.Error(400, errLibraryElementAlreadyExists.Error(), err)
	}
	if errors.Is(err, ErrLibraryElementNotFound) {
		return response.Error(404, ErrLibraryElementNotFound.Error(), err)
	}
	if errors.Is(err, errLibraryElementDashboardNotFound) {
		return response.Error(404, errLibraryElementDashboardNotFound.Error(), err)
	}
	if errors.Is(err, errLibraryElementVersionMismatch) {
		return response.Error(412, errLibraryElementVersionMismatch.Error(), err)
	}
	if errors.Is(err, dashboards.ErrFolderNotFound) {
		return response.Error(404, dashboards.ErrFolderNotFound.Error(), err)
	}
	if errors.Is(err, dashboards.ErrFolderAccessDenied) {
		return response.Error(403, dashboards.ErrFolderAccessDenied.Error(), err)
	}
	if errors.Is(err, errLibraryElementHasConnections) {
		return response.Error(403, errLibraryElementHasConnections.Error(), err)
	}
	if errors.Is(err, errLibraryElementInvalidUID) {
		return response.Error(400, errLibraryElementInvalidUID.Error(), err)
	}
	if errors.Is(err, errLibraryElementUIDTooLong) {
		return response.Error(400, errLibraryElementUIDTooLong.Error(), err)
	}
	return response.Error(500, message, err)
}

// swagger:parameters getLibraryElementByUID getLibraryElementConnections
type LibraryElementByUID struct {
	// in:path
	// required:true
	UID string `json:"library_element_uid"`
}

// swagger:parameters getLibraryElementByUID
type GetLibraryElementByUIDParams struct {
	// in:path
	// required:true
	UID string `json:"library_element_uid"`
}

// swagger:parameters GetLibraryElementConnectionsParams
type GetLibraryElementConnectionsParams struct {
	// in:path
	// required:true
	UID string `json:"library_element_uid"`
}

// swagger:parameters deleteLibraryElementByUID
type DeleteLibraryElementByUIDParams struct {
	// in:path
	// required:true
	UID string `json:"library_element_uid"`
}

// swagger:parameters getLibraryElementByName
type LibraryElementByNameParams struct {
	// in:path
	// required:true
	Name string `json:"library_element_name"`
}

// swagger:parameters getLibraryElements
type GetLibraryElementsParams struct {
	// Part of the name or description searched for.
	// in:query
	// required:false
	SearchString string `json:"searchString"`
	// Kind of element to search for.
	// in:query
	// required:false
	// Description:
	// * 1 - library panels
	// * 2 - library variables
	// enum: 1,2
	Kind int `json:"kind"`
	// Sort order of elements.
	// in:query
	// required:false
	// Description:
	// * alpha-asc: ascending
	// * alpha-desc: descending
	// Enum: alpha-asc,alpha-desc
	SortDirection string `json:"sortDirection"`
	// A comma separated list of types to filter the elements by
	// in:query
	// required:false
	TypeFilter string `json:"typeFilter"`
	// Element UID to exclude from search results.
	// in:query
	// required:false
	ExcludeUID string `json:"excludeUid"`
	// A comma separated list of folder ID(s) to filter the elements by.
	// in:query
	// required:false
	FolderFilter string `json:"folderFilter"`
	// The number of results per page.
	// in:query
	// required:false
	// default: 100
	PerPage int `json:"perPage"`
	// The page for a set of records, given that only perPage records are returned at a time. Numbering starts at 1.
	// in:query
	// required:false
	// default: 1
	Page int `json:"page"`
}

// swagger:parameters createLibraryElement
type CreateLibraryElementParams struct {
	// in:body
	// required:true
	Body CreateLibraryElementCommand `json:"body"`
}

// swagger:parameters updateLibraryElement
type UpdateLibraryElementParam struct {
	// in:body
	// required:true
	Body PatchLibraryElementCommand `json:"body"`
	// in:path
	// required:true
	UID string `json:"library_element_uid"`
}

// swagger:response getLibraryElementsResponse
type GetLibraryElementsResponse struct {
	// in: body
	Body LibraryElementSearchResponse `json:"body"`
}

// swagger:response getLibraryElementResponse
type GetLibraryElementResponse struct {
	// in: body
	Body LibraryElementResponse `json:"body"`
}

// swagger:response getLibraryElementConnectionsResponse
type GetLibraryElementConnectionsResponse struct {
	// in: body
	Body LibraryElementConnectionsResponse `json:"body"`
}
