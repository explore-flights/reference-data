package main

import (
	"context"
	_ "embed"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/goccy/go-graphviz"
	"io"
	"iter"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

//go:embed aircraft_aliases.csv
var aliases string

//go:embed aircraft_families.csv
var families string

//go:embed aircraft_types.csv
var types string

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	g, err := graphviz.New(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}

	graph, err := buildGraph(ctx, g)
	if err != nil {
		log.Fatal(err)
		return
	}

	f, err := os.Create("graph.svg")
	if err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if err := g.Render(ctx, graph, graphviz.SVG, f); err != nil {
		log.Fatal(err)
		return
	}
}

func buildGraph(ctx context.Context, g *graphviz.Graphviz) (*graphviz.Graph, error) {
	graph, err := g.Graph()
	if err != nil {
		return nil, err
	}

	graph.SetRankDir(graphviz.LRRank)

	var id graphviz.ID
	aircraftNodeById := make(map[string]*graphviz.Node)
	familyNodeById := make(map[string]*graphviz.Node)

	for _, row := range readCsv(strings.NewReader(types), &err) {
		id++
		node, err := graph.CreateNodeByName(strconv.FormatUint(uint64(id), 16))
		if err != nil {
			return nil, err
		}

		node.SetLabel(fmt.Sprintf("Aircraft\n%s\nIATA: %s\nICAO: %s", row["name"], row["iata"], row["icao"]))
		aircraftNodeById[row["id"]] = node
	}

	if err != nil {
		return nil, err
	}

	for _, row := range readCsv(strings.NewReader(families), &err) {
		id++
		node, err := graph.CreateNodeByName(strconv.FormatUint(uint64(id), 16))
		if err != nil {
			return nil, err
		}

		node.SetLabel(fmt.Sprintf("Family\n%s\nIATA: %s", row["name"], row["iata"]))
		familyNodeById[row["id"]] = node
	}

	if err != nil {
		return nil, err
	}

	for _, row := range readCsv(strings.NewReader(aliases), &err) {
		id++
		node, err := graph.CreateNodeByName(strconv.FormatUint(uint64(id), 16))
		if err != nil {
			return nil, err
		}

		node.SetLabel(fmt.Sprintf("Alias\nIATA: %s", row["alias"]))

		var targetNode *graphviz.Node
		if aircraftTypeId := row["aircraft_type"]; aircraftTypeId != "" {
			targetNode = aircraftNodeById[aircraftTypeId]
		} else if aircraftFamilyId := row["aircraft_family"]; aircraftFamilyId != "" {
			targetNode = familyNodeById[aircraftFamilyId]
		}

		if targetNode != nil {
			id++
			_, err := graph.CreateEdgeByName(strconv.FormatUint(uint64(id), 16), node, targetNode)
			if err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	for _, row := range readCsv(strings.NewReader(types), &err) {
		if familyId := row["family_id"]; familyId != "" {
			srcNode := familyNodeById[familyId]
			targetNode := aircraftNodeById[row["id"]]

			id++
			_, err := graph.CreateEdgeByName(strconv.FormatUint(uint64(id), 16), srcNode, targetNode)
			if err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	for _, row := range readCsv(strings.NewReader(families), &err) {
		if parentFamilyId := row["parent_family"]; parentFamilyId != "" {
			srcNode := familyNodeById[parentFamilyId]
			targetNode := familyNodeById[row["id"]]

			id++
			_, err := graph.CreateEdgeByName(strconv.FormatUint(uint64(id), 16), srcNode, targetNode)
			if err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return graph, nil
}

func readCsv(reader io.Reader, outErr *error) iter.Seq2[int, map[string]string] {
	return func(yield func(int, map[string]string) bool) {
		r := csv.NewReader(reader)
		headers, err := r.Read()
		if err != nil {
			*outErr = fmt.Errorf("failed to read header: %w", err)
			return
		}

		line := 1
		for {
			record, err := r.Read()
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				*outErr = err
				break
			}

			row := make(map[string]string)
			for i, colName := range headers {
				if i < len(record) {
					row[colName] = record[i]
				}
			}

			if !yield(line, row) {
				break
			}

			line++
		}
	}
}
