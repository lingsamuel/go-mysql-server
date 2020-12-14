package sql

// Visitor visits expressions in an expression tree.
type Visitor interface {
	// Visit method is invoked for each expr encountered by Walk.
	// If the result Visitor is not nil, Walk visits each of the children
	// of the expr with that visitor, followed by a call of Visit(nil)
	// to the returned visitor.
	Visit(expr Expression) Visitor
}

// Walk traverses the expression tree in depth-first order. It starts by calling
// v.Visit(expr); expr must not be nil. If the visitor returned by
// v.Visit(expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil)
// to the returned visitor.
func Walk(v Visitor, expr Expression) {
	if v = v.Visit(expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		Walk(v, child)
	}

	v.Visit(nil)
}

// NodeVisitor visits expressions in an expression tree. Like Visitor, but with the added context of the node in which
// an expression is embedded. See WalkExpressionsWithNode in the plan package.
type NodeVisitor interface {
	// Visit method is invoked for each expr encountered by Walk. If the result Visitor is not nil, Walk visits each of
	// the children of the expr with that visitor, followed by a call of Visit(nil, nil) to the returned visitor.
	Visit(node Node, expression Expression) NodeVisitor
}

// WalkWithNode traverses the expression tree in depth-first order. It starts by calling v.Visit(node, expr); expr must
// not be nil. If the visitor returned by v.Visit(node, expr) is not nil, Walk is invoked recursively with the returned
// visitor for each children of the expr, followed by a call of v.Visit(nil, nil) to the returned visitor.
func WalkWithNode(v NodeVisitor, n Node, expr Expression) {
	if v = v.Visit(n, expr); v == nil {
		return
	}

	for _, child := range expr.Children() {
		WalkWithNode(v, n, child)
	}

	v.Visit(nil, nil)
}

type inspector func(Expression) bool

func (f inspector) Visit(expr Expression) Visitor {
	if f(expr) {
		return f
	}
	return nil
}

// Inspect traverses the plan in depth-first order: It starts by calling
// f(expr); expr must not be nil. If f returns true, Inspect invokes f
// recursively for each of the children of expr, followed by a call of
// f(nil).
func Inspect(expr Expression, f func(expr Expression) bool) {
	Walk(inspector(f), expr)
}

// InspectNode traverses the plan in depth-first order: It starts by calling
// f(node); node must not be nil. If f returns true, InspectNode invokes f
// recursively for each of the children of node, followed by a call of
// f(nil).
func InspectNode(node Node, f func(node Node) bool) {
	// Since nodeWalk is only used here and altogether short, it's here to not pollute the namespace
	var nodeWalk func (v func(Node) bool, n Node)
	nodeWalk = func (v func(Node) bool, n Node) {
		if !v(n) {
			return
		}
		for _, child := range n.Children() {
			nodeWalk(v, child)
		}
		_ = v(nil)
	}
	nodeWalk(f, node)
}
