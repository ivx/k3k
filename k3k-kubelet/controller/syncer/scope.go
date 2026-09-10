package syncer

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/rancher/k3k/k3k-kubelet/translate"
	"github.com/rancher/k3k/pkg/apis/k3k.io/v1beta1"
)

// ErrRejected marks an object that must not be synced to the host because it
// would select or allow something outside its virtual cluster. It is a
// configuration problem of the virtual object, not a transient error: the
// reconciler reports it as an event and does not retry.
var ErrRejected = errors.New("rejected")

// podNamespaceKey is the label Cilium (and Kubernetes network policies in
// general) use to reference a pod's namespace in a selector.
const podNamespaceKey = "io.kubernetes.pod.namespace"

// namespaceLabelPrefixes select namespaces by *their* labels. Virtual
// namespaces are not host namespaces, so there is nothing to translate them to.
var namespaceLabelPrefixes = []string{
	"io.kubernetes.pod.namespace.labels.",
	"io.cilium.k8s.namespace.labels.",
}

// scopeSelectors rewrites every LabelSelector found at the configured paths so
// that it can only match pods of this virtual cluster, see
// CustomResourceSyncConfig.Selectors for the rules.
func scopeSelectors(root map[string]any, paths []string, clusterName, namespace string) error {
	for _, path := range paths {
		// A missing selector means "everything" for most kinds (a PDB
		// without spec.selector, an empty endpointSelector) - on the host
		// that would be every pod in the namespace, across tenants. Create
		// it so that it gets scoped like an explicit empty selector.
		if err := ensureSelectorNode(root, path); err != nil {
			return fmt.Errorf("selector path %s: %w", path, err)
		}

		nodes, err := findNodes(root, path)
		if err != nil {
			return fmt.Errorf("selector path %s: %w", path, err)
		}

		for _, node := range nodes {
			sel, ok := node.(map[string]any)
			if !ok {
				return fmt.Errorf("%w: selector at %s is not an object", ErrRejected, path)
			}

			if err := scopeSelector(sel, clusterName, namespace); err != nil {
				return fmt.Errorf("selector at %s: %w", path, err)
			}
		}
	}

	return nil
}

// ensureSelectorNode creates an empty object at path below every existing
// parent when the final segment is a fixed key that is absent. Wildcard final
// segments (list items) are left alone: an absent list selects nothing.
func ensureSelectorNode(root map[string]any, path string) error {
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("path must start with /")
	}

	i := strings.LastIndex(path, "/")
	last := path[i+1:]

	if last == "*" || last == "" {
		return nil
	}

	parentPath := path[:i]
	if parentPath == "" {
		parentPath = "/"
	}

	var parents []any

	if parentPath == "/" {
		parents = []any{root}
	} else {
		var err error
		if parents, err = findNodes(root, parentPath); err != nil {
			return err
		}
	}

	for _, p := range parents {
		if m, ok := p.(map[string]any); ok {
			if _, exists := m[last]; !exists || m[last] == nil {
				m[last] = map[string]any{}
			}
		}
	}

	return nil
}

// scopeSelector scopes one LabelSelector in place.
func scopeSelector(sel map[string]any, clusterName, namespace string) error {
	matchLabels, _ := sel["matchLabels"].(map[string]any)
	if matchLabels == nil {
		matchLabels = map[string]any{}
	}

	namespaceHandled := false

	for key, value := range matchLabels {
		if isNamespaceLabelKey(key) {
			return fmt.Errorf("%w: selecting by namespace labels (%s) cannot be translated", ErrRejected, key)
		}

		if !isNamespaceKey(key) {
			continue
		}

		ns, ok := value.(string)
		if !ok {
			return fmt.Errorf("%w: namespace reference %s is not a string", ErrRejected, key)
		}

		delete(matchLabels, key)
		matchLabels[translate.NamespaceNameLabel] = ns
		namespaceHandled = true
	}

	if exprs, ok := sel["matchExpressions"].([]any); ok {
		for _, e := range exprs {
			expr, ok := e.(map[string]any)
			if !ok {
				continue
			}

			key, _ := expr["key"].(string)
			if isNamespaceLabelKey(key) {
				return fmt.Errorf("%w: selecting by namespace labels (%s) cannot be translated", ErrRejected, key)
			}

			if !isNamespaceKey(key) {
				continue
			}

			if op, _ := expr["operator"].(string); op != "In" {
				return fmt.Errorf("%w: namespace reference %s with operator %s cannot be translated (only In)", ErrRejected, key, op)
			}

			expr["key"] = translate.NamespaceNameLabel
			namespaceHandled = true
		}
	}

	matchLabels[translate.ClusterNameLabel] = clusterName

	if !namespaceHandled {
		matchLabels[translate.NamespaceNameLabel] = namespace
	}

	sel["matchLabels"] = matchLabels

	return nil
}

// isNamespaceKey reports whether a selector key references the pod namespace,
// with or without a Cilium label-source prefix (k8s:, any:), or is already the
// translated form.
func isNamespaceKey(key string) bool {
	return stripSourcePrefix(key) == podNamespaceKey || key == translate.NamespaceNameLabel
}

func isNamespaceLabelKey(key string) bool {
	key = stripSourcePrefix(key)

	for _, p := range namespaceLabelPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}

	return false
}

func stripSourcePrefix(key string) string {
	if i := strings.Index(key, ":"); i > 0 && !strings.Contains(key[:i], "/") && !strings.Contains(key[:i], ".") {
		return key[i+1:]
	}

	return key
}

// checkRejects verifies that none of the configured fields carries a value
// outside its allow list.
func checkRejects(root map[string]any, rejects []v1beta1.CustomResourceReject) error {
	for _, rule := range rejects {
		nodes, err := findNodes(root, rule.Path)
		if err != nil {
			return fmt.Errorf("reject path %s: %w", rule.Path, err)
		}

		for _, node := range nodes {
			if isEmpty(node) {
				continue
			}

			if allowed(node, rule.Allow) {
				continue
			}

			return fmt.Errorf("%w: %s may not be set (value %v)", ErrRejected, rule.Path, node)
		}
	}

	return nil
}

func isEmpty(node any) bool {
	switch v := node.(type) {
	case nil:
		return true
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	case string:
		return v == ""
	default:
		return false
	}
}

// allowed reports whether node is a list of strings that are all in allow.
func allowed(node any, allow []string) bool {
	if len(allow) == 0 {
		return false
	}

	items, ok := node.([]any)
	if !ok {
		return false
	}

	for _, item := range items {
		s, ok := item.(string)
		if !ok || !slices.Contains(allow, s) {
			return false
		}
	}

	return true
}

// findNodes returns every value addressed by a JSON-pointer path in which a
// "*" segment matches all items of a list or all values of a map. Missing
// segments match nothing; they are not an error.
func findNodes(root any, path string) ([]any, error) {
	if !strings.HasPrefix(path, "/") {
		return nil, fmt.Errorf("path must start with /")
	}

	segments := strings.Split(path[1:], "/")
	for i, s := range segments {
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
	}

	current := []any{root}

	for _, seg := range segments {
		var next []any

		for _, node := range current {
			switch n := node.(type) {
			case map[string]any:
				if seg == "*" {
					for _, v := range n {
						next = append(next, v)
					}
				} else if v, ok := n[seg]; ok {
					next = append(next, v)
				}
			case []any:
				if seg == "*" {
					next = append(next, n...)
				} else if idx, err := parseIndex(seg, len(n)); err == nil {
					next = append(next, n[idx])
				}
			}
		}

		current = next
	}

	return current, nil
}

func parseIndex(seg string, length int) (int, error) {
	idx := 0

	for _, c := range seg {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not an index")
		}

		idx = idx*10 + int(c-'0')
	}

	if seg == "" || idx >= length {
		return 0, fmt.Errorf("index out of range")
	}

	return idx, nil
}
