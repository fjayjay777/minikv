package engine

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type IndexBuilder interface {
	BuildIndex(key string) error
	ReadIndex(key string) (int, error)
}

type store struct {
	storePath    string
	indexBuilder IndexBuilder
}

func NewStore(storePath string, indexBuilder IndexBuilder) *store {
	return &store{
		storePath:    storePath,
		indexBuilder: indexBuilder,
	}
}

func (s *store) StoreKV(key string, value string) (int, error) {
	file, err := os.OpenFile(s.storePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	data := fmt.Sprintf("%s : %s,\n", key, value)

	n, err := file.WriteString(data)
	if err != nil {
		return -1, err
	}

	err = s.indexBuilder.BuildIndex(key)
	if err != nil {
		return -1, err
	}

	return n, nil
}

func (s *store) ReadKey(key string) (string, error) {
	file, err := os.Open(s.storePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	record, _, err := findRecordInFile(key, file)
	if err != nil {
		return "", err
	}
	return record, nil
}

func (s *store) DeleteKey(key string) error {
	file, err := os.OpenFile(s.storePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, indexes, err := findRecordInFile(key, file)
	if err != nil {
		return err
	}

	records, err := readRecordsFromFile(file)
	if err != nil {
		return err
	}

	rest, err := removeRecordsFromList(records, indexes)
	if err != nil {
		return err
	}

	err = writeRecordsToFile(rest, file)
	if err != nil {
		return err
	}
	return nil
}

func writeRecordsToFile(records []string, file *os.File) error {
	writer := bufio.NewWriter(file)
	for _, line := range records {
		_, err := fmt.Fprintln(writer, line)
		if err != nil {
			return err
		}
	}

	return writer.Flush()
}

func removeRecordsFromList(records []string, indexes []int) ([]string, error) {
	var rest []string
	for i, record := range records {
		isCurIndex := false
		for _, index := range indexes {
			if index < 1 || index > len(records) {
				return nil, fmt.Errorf("unable to remove record because index %d is out of bound", index)
			}
			if i == index {
				isCurIndex = true
			}
		}
		if isCurIndex {
			continue
		}
		rest = append(rest, record)
	}

	return rest, nil
}

func readRecordsFromFile(file *os.File) ([]string, error) {
	var records []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		records = append(records, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

func findRecordInFile(key string, file *os.File) (string, []int, error) {
	scanner := bufio.NewScanner(file)

	var found bool
	var foundVal string
	var foundIndexes []int
	var curLine int

	for scanner.Scan() {
		curLine++
		line := scanner.Text()
		split := strings.Split(line, ":")
		curKey := strings.TrimSpace(split[0])
		curVal := strings.TrimSpace(split[1])
		if curKey == key {
			found = true
			foundVal = curVal
			foundIndexes = append(foundIndexes, curLine)
		}
	}

	if !found {
		return "", nil, fmt.Errorf("no record found")
	}

	return foundVal, foundIndexes, nil
}
