package dynamorm

import "errors"

var ErrNotFound = errors.New("not found")

var IncompatibleModelerError = errors.New("modeler does not support this item")
