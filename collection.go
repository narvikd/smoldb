package smoldb

import (
	"encoding/json"
	"errors"
	"github.com/narvikd/filekit"
	"sync"
)

type Collection struct {
	sync.RWMutex
	Records         map[string]string
	LastRecordsHash string
}

func newCollection() (*Collection, error) {
	c := &Collection{
		Records: make(map[string]string),
	}
	if !filekit.FileExist(filePath) {
		return c, nil
	}

	f, errReadFile := readCompressedFile(filePath)
	if errReadFile != nil {
		return nil, errWrap(errReadFile, "couldn't read DB file")
	}

	errUnmarshal := json.Unmarshal(f, &c.Records)
	if errUnmarshal != nil {
		return nil, errWrap(errUnmarshal, "couldn't unmarshal DB file into records")
	}

	return c, nil
}

func (c *Collection) NewRecord(key string, value string) error {
	_, exist := c.Records[key]
	if exist {
		return errors.New("record already exist")
	}

	c.Records[key] = value
	return nil
}

func (c *Collection) GetRecord(key string) (string, error) {
	_, exist := c.Records[key]
	if !exist {
		return "", errors.New("record doesn't exist")
	}
	return c.Records[key], nil
}

func (c *Collection) ModifyRecord(key string, value string) error {
	_, exist := c.Records[key]
	if !exist {
		return errors.New("record doesn't exist")
	}

	c.Records[key] = value
	return nil
}

func (c *Collection) DelRecord(key string) error {
	_, exist := c.Records[key]
	if !exist {
		return errors.New("record doesn't exist")
	}

	delete(c.Records, key)
	return nil
}

func (c *Collection) GetAllRecordsKeys() []string {
	var s []string
	for k := range c.Records {
		s = append(s, k)
	}
	return s
}

func (c *Collection) GetAllRecords() map[string]string {
	return c.Records
}
