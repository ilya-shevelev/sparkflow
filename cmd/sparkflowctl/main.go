// Command sparkflowctl is the CLI tool for interacting with a Sparkflow server.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/ilya-shevelev/sparkflow/pkg/dag"
	"github.com/ilya-shevelev/sparkflow/pkg/executor"
	"github.com/ilya-shevelev/sparkflow/pkg/scheduler"
	"github.com/ilya-shevelev/sparkflow/pkg/store"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)

	switch cmd {
	case "version":
		fmt.Printf("sparkflowctl %s (commit: %s)\n", version, commit)
	case "validate":
		cmdValidate()
	case "run":
		cmdRun()
	case "list":
		cmdList()
	case "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`sparkflowctl - Sparkflow CLI

Usage:
  sparkflowctl <command> [options]

Commands:
  validate    Validate a DAG YAML file
  run         Run a DAG locally
  list        List DAGs
  version     Show version
  help        Show this help

Examples:
  sparkflowctl validate -f workflow.yaml
  sparkflowctl run -f workflow.yaml -p date=2024-01-01
  sparkflowctl list`)
}

func cmdValidate() {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)
	file := fs.String("f", "", "DAG YAML file to validate")
	outputJSON := fs.Bool("json", false, "Output validation result as JSON")
	_ = fs.Parse(os.Args[1:])

	if *file == "" {
		fmt.Fprintln(os.Stderr, "error: -f flag is required")
		os.Exit(1)
	}

	d, err := dag.ParseFile(*file)
	if err != nil {
		if *outputJSON {
			result := map[string]any{
				"valid": false,
				"error": err.Error(),
				"file":  *file,
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(result)
		} else {
			fmt.Fprintf(os.Stderr, "INVALID: %s\n  %s\n", *file, err)
		}
		os.Exit(1)
	}

	sorted, err := d.TopologicalSort()
	if err != nil {
		fmt.Fprintf(os.Stderr, "INVALID: %s\n  %s\n", *file, err)
		os.Exit(1)
	}

	if *outputJSON {
		taskOrder := make([]string, len(sorted))
		for i, t := range sorted {
			taskOrder[i] = t.ID
		}
		result := map[string]any{
			"valid":      true,
			"file":       *file,
			"dag_id":     d.ID,
			"name":       d.Name,
			"tasks":      len(d.Tasks),
			"task_order": taskOrder,
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(result)
	} else {
		fmt.Printf("VALID: %s\n", *file)
		fmt.Printf("  DAG: %s (%s)\n", d.ID, d.Name)
		fmt.Printf("  Tasks: %d\n", len(d.Tasks))
		fmt.Printf("  Execution order:\n")
		for i, t := range sorted {
			fmt.Printf("    %d. %s (%s)\n", i+1, t.ID, t.Executor)
		}
	}
}

func cmdRun() {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	file := fs.String("f", "", "DAG YAML file to run")
	_ = fs.Parse(os.Args[1:])

	if *file == "" {
		fmt.Fprintln(os.Stderr, "error: -f flag is required")
		os.Exit(1)
	}

	d, err := dag.ParseFile(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Running DAG: %s (%s)\n", d.ID, d.Name)
	fmt.Println("---")

	// Execute locally using in-memory store and shell executor.
	st := store.NewMemoryStore()
	_ = st.SaveDAG(context.Background(), d)

	sch := scheduler.NewFIFOScheduler(st, scheduler.FIFOConfig{})
	ctx := context.Background()

	runID, err := sch.Submit(ctx, d, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error submitting: %s\n", err)
		os.Exit(1)
	}

	reg := executor.NewRegistry()
	reg.Register(executor.NewShellExecutor())
	reg.Register(executor.NewPythonExecutor(executor.PythonConfig{}))

	fmt.Printf("Run ID: %s\n\n", runID)

	for {
		ready, err := sch.Schedule(ctx)
		if err != nil {
			fmt.Fprintf(os.Stderr, "scheduling error: %s\n", err)
			os.Exit(1)
		}

		if len(ready) == 0 {
			// Check if all done.
			run, _ := st.GetRun(ctx, runID)
			if run != nil && run.Status.IsTerminal() {
				fmt.Println("---")
				fmt.Printf("Run completed: %s\n", run.Status)
				if run.Status == dag.StatusFailed {
					os.Exit(1)
				}
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		for _, ti := range ready {
			task := d.Tasks[ti.TaskID]
			exec, err := reg.Get(task.Executor)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  [%s] executor not found: %s\n", ti.TaskID, err)
				_ = sch.ReportCompletion(ctx, ti.TaskID, &scheduler.TaskResult{
					TaskInstanceID: ti.ID,
					RunID:          runID,
					Status:         dag.StatusFailed,
					Error:          err.Error(),
				})
				continue
			}

			fmt.Printf("  [%s] executing (%s)...\n", ti.TaskID, task.Executor)
			result, execErr := exec.Execute(ctx, task, nil)

			if execErr != nil {
				fmt.Fprintf(os.Stderr, "  [%s] error: %s\n", ti.TaskID, execErr)
				_ = sch.ReportCompletion(ctx, ti.TaskID, &scheduler.TaskResult{
					TaskInstanceID: ti.ID,
					RunID:          runID,
					Status:         dag.StatusFailed,
					Error:          execErr.Error(),
				})
			} else {
				fmt.Printf("  [%s] %s\n", ti.TaskID, result.Status)
				if len(result.Output) > 0 {
					fmt.Printf("  [%s] output: %s\n", ti.TaskID, string(result.Output))
				}
				errStr := ""
				if result.Error != nil {
					errStr = result.Error.Error()
				}
				_ = sch.ReportCompletion(ctx, ti.TaskID, &scheduler.TaskResult{
					TaskInstanceID: ti.ID,
					RunID:          runID,
					Status:         result.Status,
					Output:         result.Output,
					Error:          errStr,
				})
			}
		}
	}
}

func cmdList() {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	dir := fs.String("d", ".", "Directory to search for DAG YAML files")
	_ = fs.Parse(os.Args[1:])

	entries, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading directory: %s\n", err)
		os.Exit(1)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FILE\tDAG ID\tNAME\tTASKS\tSCHEDULE")

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if len(name) < 5 {
			continue
		}
		ext := name[len(name)-4:]
		if ext != ".yml" && ext != "yaml" && name[len(name)-5:] != ".yaml" {
			continue
		}

		path := *dir + "/" + name
		d, err := dag.ParseFile(path)
		if err != nil {
			fmt.Fprintf(w, "%s\tERROR\t%s\t-\t-\n", name, err)
			continue
		}

		schedule := "-"
		if d.Schedule != nil {
			schedule = d.Schedule.Expression
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", name, d.ID, d.Name, len(d.Tasks), schedule)
	}

	w.Flush()
}
