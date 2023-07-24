package main

import (
	"encoding/json"
	"os"
)

type indexMeta struct {
	Key    string `json:"key"`
	Offset int    `json:"offset"`
}

type sst struct {
	storePath string
}

func NewSST(storePath string) *sst {
	return &sst{
		storePath: storePath,
	}
}

func (s *sst) WriteKeyIndex(key string, valLen int) error {
	lastOffset, err := s.getLastOffset()
	if err != nil {
		return err
	}

	indexMeta := &indexMeta{
		Key:    key,
		Offset: lastOffset + valLen,
	}
	jsonData, err := json.MarshalIndent(indexMeta, "", "    ")
	if err != nil {
		return err
	}

	file, err := os.OpenFile(s.storePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, err = file.Write(append(jsonData, []byte("\n")...))
	if err != nil {
		file.Close()
		return err
	}

	return nil
}

func (s *sst) ReadKeyIndex(key string) (string, error) {
	
	return "", nil
}

func (s *sst) getLastOffset() (int, error) {
	indexMetaList, err := s.readIndexMeta()
	if err != nil {
		return -1, err
	}
	if len(indexMetaList) == 0 {
		return 0, nil
	}

	lastIndexMeta := indexMetaList[len(indexMetaList)-1]
	return lastIndexMeta.Offset, nil
}

func (s *sst) readIndexMeta() ([]indexMeta, error) {
	file, err := os.Open(s.storePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var indexMetaList []indexMeta

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var indexMeta indexMeta
		if err := decoder.Decode(&indexMeta); err != nil {
			return nil, err
		}
		indexMetaList = append(indexMetaList, indexMeta)
	}
	return indexMetaList, nil
}
