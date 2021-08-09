package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

type StandAloneReader struct {
	txn  *badger.Txn
	iter engine_util.DBIterator
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	r.iter = engine_util.NewCFIterator(cf, r.txn)
	return r.iter
}

func (r *StandAloneReader) Close() {
	if r.iter != nil {
		r.iter.Close()
	}
	r.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	return &StandAloneStorage{db}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	r := &StandAloneReader{txn: s.db.NewTransaction(false)}
	return r, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for i := range batch {
		m := batch[i]

		switch m.Data.(type) {
		case storage.Put:
			key := m.Key()
			value := m.Value()
			cf := m.Cf()
			err := engine_util.PutCF(s.db, cf, key, value)
			if err != nil {
				return err
			}
		case storage.Delete:
			key := m.Key()
			cf := m.Cf()
			err := engine_util.DeleteCF(s.db, cf, key)
			if err != nil {
				return err
			}
		default:
			return errors.New("undefined operation")
		}
	}
	return nil
}
