package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const exampleJSON = `{
  "streams": [
    {"id": "a", "weight": 2},
    {"id": "b", "weight": 1}
  ],
  "packets": [
    {"id": "p1", "stream": "a", "arrival": 0, "duration": 4},
    {"id": "p2", "stream": "b", "arrival": 0, "duration": 2},
    {"id": "p3", "stream": "a", "arrival": 1, "duration": 2},
    {"id": "p4", "stream": "b", "arrival": 9, "duration": 1}
  ]
}`

func TestRunStdin(t *testing.T) {
	var buf bytes.Buffer
	if err := run(nil, strings.NewReader(exampleJSON), &buf); err != nil {
		t.Fatalf("run: %v", err)
	}
	var got Output
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	var in Input
	if err := json.Unmarshal([]byte(exampleJSON), &in); err != nil {
		t.Fatalf("bad test fixture: %v", err)
	}
	if want := schedule(&in); !reflect.DeepEqual(&got, want) {
		t.Errorf("CLI output differs from schedule()\ngot:  %+v\nwant: %+v", &got, want)
	}
	checkOutput(t, &in, &got)
}

func TestRunFiles(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "in.json")
	outPath := filepath.Join(dir, "out.json")
	if err := os.WriteFile(inPath, []byte(exampleJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"-o", outPath, inPath}, nil, io.Discard); err != nil {
		t.Fatalf("run: %v", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	var got Output
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("output file is not valid JSON: %v", err)
	}
	if len(got.Packets) != 4 {
		t.Errorf("got %d packet records, want 4", len(got.Packets))
	}
}

func TestRunErrors(t *testing.T) {
	cases := map[string]string{
		"not json":       `not json`,
		"empty":          ``,
		"no streams":     `{"streams":[],"packets":[]}`,
		"bad weight":     `{"streams":[{"id":"a","weight":0},{"id":"b","weight":1}],"packets":[]}`,
		"unknown stream": `{"streams":[{"id":"a","weight":1},{"id":"b","weight":1}],"packets":[{"id":"p","stream":"z","arrival":0,"duration":1}]}`,
		"unsorted":       `{"streams":[{"id":"a","weight":1},{"id":"b","weight":1}],"packets":[{"id":"p1","stream":"a","arrival":5,"duration":1},{"id":"p2","stream":"a","arrival":2,"duration":1}]}`,
	}
	for name, input := range cases {
		if err := run(nil, strings.NewReader(input), io.Discard); err == nil {
			t.Errorf("%s: want error, got nil", name)
		}
	}
	if err := run([]string{"a.json", "b.json"}, nil, io.Discard); err == nil {
		t.Error("two positional args: want error, got nil")
	}
}
