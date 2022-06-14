package smoldb

import (
	"github.com/stretchr/testify/require"
	"log"
	"os"
	"strconv"
	"testing"
)

func reset() {
	_ = os.RemoveAll(filePath)
	err := setSingleton()
	if err != nil {
		log.Fatalln(err)
	}
}

func init() {
	err := setSingleton()
	if err != nil {
		log.Fatalln(err)
	}
}

func TestNewRecord(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"
	err := DB.NewRecord(key, value)
	require.Equal(t, nil, err)
}

func TestRecordAlreadyExist(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"
	errRecord1 := DB.NewRecord(key, value)
	if errRecord1 != nil {
		require.FailNow(t, "errRecord1", errRecord1)
	}
	errRecord2 := DB.NewRecord(key, value)
	require.NotEqual(t, nil, errRecord2)
}

func TestGetRecord(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"

	errNewRecord := DB.NewRecord(key, value)
	if errNewRecord != nil {
		require.FailNow(t, "errNewRecord", errNewRecord)
	}

	recordValue, errGetRecord := DB.GetRecord(key)
	if errGetRecord != nil {
		require.FailNow(t, "errGetRecord", errGetRecord)
	}

	require.Equal(t, value, recordValue)
}

func TestModifyRecord(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"
	const newValue = "newValue"

	errNewRecord := DB.NewRecord(key, value)
	if errNewRecord != nil {
		require.FailNow(t, "errNewRecord", errNewRecord)
	}

	errModifyRecord := DB.ModifyRecord(key, newValue)
	if errModifyRecord != nil {
		require.FailNow(t, "errModifyRecord", errModifyRecord)
	}

	recordValue, errGetRecord := DB.GetRecord(key)
	if errGetRecord != nil {
		require.FailNow(t, "errGetRecord", errGetRecord)
	}

	require.Equal(t, newValue, recordValue)
}

func TestModifyRecordDoesntExist(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"
	const newValue = "newValue"

	errNewRecord := DB.NewRecord(key, value)
	if errNewRecord != nil {
		require.FailNow(t, "errNewRecord", errNewRecord)
	}

	errModifyRecord := DB.ModifyRecord(key+"1", newValue)
	require.NotEqual(t, nil, errModifyRecord)
}

func TestDeleteRecord(t *testing.T) {
	defer reset()
	const key = "record122344"
	const value = "value122344"

	errNewRecord := DB.NewRecord(key, value)
	if errNewRecord != nil {
		require.FailNow(t, "errNewRecord", errNewRecord)
	}

	recordValue, errGetRecord := DB.GetRecord(key)
	if errGetRecord != nil {
		require.FailNow(t, "errGetRecord", errGetRecord)
	}
	require.Equal(t, value, recordValue)

	errDeleteExistentRecord := DB.DelRecord(key)
	if errDeleteExistentRecord != nil {
		require.FailNow(t, "errDeleteExistentRecord", errDeleteExistentRecord)
	}

	errDeleteNonExistentRecord := DB.DelRecord(key)
	if errDeleteNonExistentRecord == nil {
		require.FailNow(t, "errDeleteNonExistentRecord", errDeleteNonExistentRecord)
	}

	_, errGetNewRecord := DB.GetRecord(key)
	if errGetNewRecord == nil {
		require.FailNow(t, "errGetNewRecord", errGetNewRecord)
	}
}

func TestLenAllRecordsAndList(t *testing.T) {
	defer reset()
	const iterations = 100
	for i := 0; i < iterations; i++ {
		num, _ := numBetween(9999999, 99999999999999)
		key := "key" + strconv.Itoa(int(num))
		err := DB.NewRecord(key, "me")
		if err != nil {
			log.Fatalln(err)
		}
	}
	require.Equal(t, 100, len(DB.GetAllRecords()))
	require.Equal(t, 100, len(DB.GetAllRecordsKeys()))
}
