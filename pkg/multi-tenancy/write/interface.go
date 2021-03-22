package write

// WriteAuthorizer is an authorizer that tells if a write request is authorized to be written.
type WriteAuthorizer interface {
	IsAuthorized() bool
}
