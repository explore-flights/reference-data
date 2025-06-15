package main

import (
	"io"
	"strings"
	"testing"
)

type readerAndIdColumn struct {
	reader    io.Reader
	idColumn  string
	allowNull bool
}

func TestIdsAreUnique(t *testing.T) {
	testIdsAreUnique(t, readerAndIdColumn{reader: strings.NewReader(aliases), idColumn: "alias"})
	testIdsAreUnique(t, readerAndIdColumn{reader: strings.NewReader(families), idColumn: "id"})
	testIdsAreUnique(t, readerAndIdColumn{reader: strings.NewReader(types), idColumn: "id"})
	testIdsAreUnique(
		t,
		readerAndIdColumn{reader: strings.NewReader(types), idColumn: "iata"},
		readerAndIdColumn{reader: strings.NewReader(aliases), idColumn: "alias"},
		readerAndIdColumn{reader: strings.NewReader(families), idColumn: "iata", allowNull: true},
	)
}

func TestAliasesXor(t *testing.T) {
	var err error
	for line, row := range readCsv(strings.NewReader(aliases), &err) {
		isType := row["aircraft_type"] != ""
		isFamily := row["aircraft_family"] != ""

		if isType && isFamily {
			t.Fatalf("both type and family are set in line %d", line)
			return
		} else if !isType && !isFamily {
			t.Fatalf("neither type nor family are set in line %d", line)
			return
		}
	}

	if err != nil {
		t.Fatal(err)
		return
	}
}

func TestReferences(t *testing.T) {
	expectedFamilyIds := make(map[string]struct{})
	expectedAircraftIds := make(map[string]struct{})

	var err error
	for _, row := range readCsv(strings.NewReader(aliases), &err) {
		if aircraftId := row["aircraft_type"]; aircraftId != "" {
			expectedAircraftIds[aircraftId] = struct{}{}
		}

		if familyId := row["aircraft_family"]; familyId != "" {
			expectedFamilyIds[familyId] = struct{}{}
		}
	}

	if err != nil {
		t.Fatal(err)
		return
	}

	for _, row := range readCsv(strings.NewReader(types), &err) {
		if familyId := row["family_id"]; familyId != "" {
			expectedFamilyIds[familyId] = struct{}{}
		}

		delete(expectedAircraftIds, row["id"])
	}

	if err != nil {
		t.Fatal(err)
		return
	}

	if len(expectedAircraftIds) > 0 {
		t.Fatalf("missing expected aircraft ids: %v", expectedAircraftIds)
		return
	}

	for _, row := range readCsv(strings.NewReader(families), &err) {
		if familyId := row["parent_family"]; familyId != "" {
			expectedFamilyIds[familyId] = struct{}{}
		}
	}

	if err != nil {
		t.Fatal(err)
		return
	}

	for _, row := range readCsv(strings.NewReader(families), &err) {
		delete(expectedFamilyIds, row["id"])
	}

	if err != nil {
		t.Fatal(err)
		return
	}

	if len(expectedFamilyIds) > 0 {
		t.Fatalf("missing expected family ids: %v", expectedFamilyIds)
		return
	}
}

func testIdsAreUnique(t *testing.T, readersAndIdColumns ...readerAndIdColumn) {
	var err error
	ids := make(map[string]struct{})
	for _, readerAndIdColumn := range readersAndIdColumns {
		for line, row := range readCsv(readerAndIdColumn.reader, &err) {
			id := row[readerAndIdColumn.idColumn]
			if id == "" {
				if !readerAndIdColumn.allowNull {
					t.Fatalf("%s is null in line %d", readerAndIdColumn.idColumn, line)
				}
			} else {
				if _, ok := ids[id]; ok {
					t.Fatalf("duplicate %s: %q in line %d", readerAndIdColumn.idColumn, id, line)
					return
				}

				ids[id] = struct{}{}
			}
		}
	}

	if err != nil {
		t.Fatal(err)
		return
	}
}
