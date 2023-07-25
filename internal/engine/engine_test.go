package engine

import "testing"

func TestRemoveRecordsFromList(t *testing.T) {
	original := []string{"10", "20", "30", "10", "20", "10"}
	indexes := []int{1, 2, 4}
	expected := []string{"10", "10", "10"}
	got, err := removeRecordsFromList(original, indexes)
	if err != nil {
		t.Fatal(err)
	}
	if !areSlicesEqual(got, expected) {
		t.Fatalf("got %v, and expected %v", got, expected)
	}
}

func areSlicesEqual(slice1, slice2 []string) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	for i, v := range slice1 {
		if v != slice2[i] {
			return false
		}
	}

	return true
}
