package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "packets: "+err.Error())
		os.Exit(1)
	}
}

// run executes the CLI: packets [-o output.json] [input.json]
// With no file argument the problem is read from stdin.
func run(args []string, stdin io.Reader, stdout io.Writer) error {
	fs := flag.NewFlagSet("packets", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outPath := fs.String("o", "", "write the schedule to `file` instead of stdout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return fmt.Errorf("usage: packets [-o output.json] [input.json]")
	}

	in, err := readInput(fs.Arg(0), stdin)
	if err != nil {
		return err
	}
	if err := validate(in); err != nil {
		return fmt.Errorf("invalid input: %w", err)
	}

	out := schedule(in)

	w := stdout
	if *outPath != "" {
		f, err := os.Create(*outPath)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

// readInput decodes one JSON problem from path, or from stdin when path
// is empty.
func readInput(path string, stdin io.Reader) (*Input, error) {
	r := stdin
	if path != "" {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		r = f
	}
	var in Input
	if err := json.NewDecoder(r).Decode(&in); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	return &in, nil
}
