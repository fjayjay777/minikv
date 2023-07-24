package engine

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type store struct {
	storePath string
}

func NewStore(storePath string) *store {
	return &store{
		storePath: storePath,
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
	return n, nil
}

func (s *store) ReadKey(key string) (string, int, error) {
	store, err := os.Open(s.storePath)
	if err != nil {
		return "", 0, err
	}
	defer store.Close()
	scanner := bufio.NewScanner(store)

	var found bool
	var foundVal string
	var foundCount int

	for scanner.Scan() {
		line := scanner.Text()
		split := strings.Split(line, ":")
		curKey := strings.TrimSpace(split[0])
		curVal := strings.TrimSpace(split[1])
		if curKey == key {
			found = true
			foundVal = curVal
			foundCount++
		}
	}

	if !found {
		return "", 0, fmt.Errorf("no record found")
	}

	return foundVal, foundCount, nil
}
