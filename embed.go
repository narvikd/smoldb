package smoldb

import (
	"encoding/json"
	"errors"
	"github.com/narvikd/filekit"
	"log"
	"sync"
	"time"
)

const filePath = "db.json.zstd"

var (
	once  sync.Once
	DB    *Collection
	debug = false
)

func New() (*Collection, error) {
	d, err := newDB()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func NewDebug() (*Collection, error) {
	debug = true
	d, err := newDB()
	if err != nil {
		return nil, err
	}
	return d, nil
}

func newDB() (*Collection, error) {
	if DB != nil {
		return nil, errors.New("DB already initialized")
	}
	var errSetSingleton error

	once.Do(func() {
		err := setSingleton()
		if err != nil {
			errSetSingleton = err
		} else {
			go DB.watcher()
		}
	})

	if errSetSingleton != nil {
		return nil, errSetSingleton
	}

	return DB, nil
}

func setSingleton() error {
	collection, err := newCollection()
	if err != nil {
		return err
	}
	DB = collection
	return nil
}

func (c *Collection) getLastRecordHash() string {
	c.RLock()
	defer c.RUnlock()
	return c.LastRecordsHash
}

func (c *Collection) setLastRecordHash(h string) {
	c.Lock()
	defer c.Unlock()
	c.LastRecordsHash = h
}

func (c *Collection) watcher() {
	for {
		if len(c.GetAllRecords()) > 0 {
			if c.LastRecordsHash == "" { // Is not set
				c.SaveRecordsAndSetHash() // Set it and save the records file
			} else { // If it is set
				if c.getHash(c.GetAllRecords()) != c.getLastRecordHash() { // But a new hash isn't the same as the stored one
					c.SaveRecordsAndSetHash() // Set it and save the records file
				}
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func (c *Collection) SaveRecordsAndSetHash() {
	records := c.GetAllRecords()
	err := c.saveRecordsToFile(records)
	if err != nil {
		log.Println("DB: there was a problem saving the records file:", err)
	}
	c.setLastRecordHash(c.getHash(records))
	if debug {
		log.Println("DB: Debug: Records modified... Saving")
	}
}

func (c *Collection) getHash(records map[string]string) string {
	c.RLock()
	defer c.RUnlock()
	h, errH := hashInput(records)
	if errH != nil {
		log.Println("DB: there was a problem hashing records:", errH)
	}
	return h
}

func (c *Collection) saveRecordsToFile(records map[string]string) error {
	c.Lock()
	defer c.Unlock()
	jsonBytes, errMarshal := json.Marshal(records)
	if errMarshal != nil {
		return errWrap(errMarshal, "DB: couldn't marshal records")
	}

	compressedJsonBytes, errCompress := compress(jsonBytes)
	if errCompress != nil {
		return errWrap(errMarshal, "DB: couldn't compress records")
	}

	errSaveJsonFile := filekit.WriteToFile(filePath, compressedJsonBytes)
	if errSaveJsonFile != nil {
		return errWrap(errSaveJsonFile, "DB: couldn't write records")
	}
	return nil
}
