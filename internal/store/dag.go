package store

import "fmt"

// ValidateDependencies checks that all referenced job IDs exist and that
// adding a new job with the given dependencies would not create a cycle.
// Returns nil if valid, or a descriptive error if not.
func ValidateDependencies(s JobStore, dependsOn []string) error {
	if len(dependsOn) == 0 {
		return nil
	}

	// Check all referenced IDs exist.
	for _, depID := range dependsOn {
		if _, found := s.GetJob(depID); !found {
			return fmt.Errorf("dependency %q does not exist", depID)
		}
	}

	// Build adjacency list: node -> list of nodes it depends on.
	// Include all existing jobs plus a temporary node for the new job.
	graph := make(map[string][]string)
	for _, job := range s.ListJobs() {
		graph[job.ID] = job.DependsOn
	}

	const newJobID = "__new__"
	graph[newJobID] = dependsOn

	// DFS cycle detection using three-color marking:
	// white = unvisited, gray = in current path, black = fully processed.
	// If DFS reaches a gray node, we have found a cycle.
	const (
		white = 0
		gray  = 1
		black = 2
	)

	color := make(map[string]int) // defaults to white (0)
	// path tracks the current DFS stack for descriptive error messages.
	var path []string

	var dfs func(node string) error
	dfs = func(node string) error {
		color[node] = gray
		path = append(path, node)

		for _, dep := range graph[node] {
			switch color[dep] {
			case gray:
				// Found a cycle. Build a readable cycle string
				// starting from the repeated node.
				return fmt.Errorf("cycle detected: %s", formatCycle(path, dep))
			case white:
				if err := dfs(dep); err != nil {
					return err
				}
			}
			// black: already fully explored, skip.
		}

		path = path[:len(path)-1]
		color[node] = black
		return nil
	}

	// Start DFS from the new job. We only need to check reachability
	// from the new node since existing jobs were validated at their
	// own submission time.
	return dfs(newJobID)
}

// formatCycle builds a readable cycle string like "A -> B -> C -> A".
// path is the current DFS stack, target is the node that was seen again.
func formatCycle(path []string, target string) string {
	// Find where the cycle starts in the path.
	start := -1
	for i, node := range path {
		if node == target {
			start = i
			break
		}
	}

	result := ""
	for i := start; i < len(path); i++ {
		if i > start {
			result += "->"
		}
		result += path[i]
	}
	result += "->" + target
	return result
}
