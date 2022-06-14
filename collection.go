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
	c.Lock()
	defer c.Unlock()
	_, exist := c.Records[key]
	if exist {
		return errors.New("record already exist")
	}

	c.Records[key] = value
	return nil
}

func (c *Collection) GetRecord(key string) (string, error) {
	c.RLock()
	defer c.RUnlock()
	v, exist := c.Records[key]
	if !exist {
		return "", errors.New("record doesn't exist")
	}
	return v, nil
}

func (c *Collection) ModifyRecord(key string, value string) error {
	c.Lock()
	defer c.Unlock()
	_, exist := c.Records[key]
	if !exist {
		return errors.New("record doesn't exist")
	}

	c.Records[key] = value
	return nil
}

func (c *Collection) DelRecord(key string) error {
	c.Lock()
	defer c.Unlock()
	_, exist := c.Records[key]
	if !exist {
		return errors.New("record doesn't exist")
	}

	delete(c.Records, key)
	return nil
}

func (c *Collection) GetAllRecordsKeys() []string {
	c.RLock()
	defer c.RUnlock()
	var s []string
	for k := range c.Records {
		s = append(s, k)
	}
	return s
}

func (c *Collection) GetAllRecords() map[string]string {
	c.RLock()
	defer c.RUnlock()
	// It needs to be a copy of the map, otherwise it will get into a race condition
	newMap := make(map[string]string)
	for k, v := range c.Records {
		newMap[k] = v
	}
	return newMap
}
