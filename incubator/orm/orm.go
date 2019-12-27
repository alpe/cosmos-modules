package orm

import (
	"io"
	"reflect"

	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/store/prefix"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/errors"
)

const ormCodespace = "orm"

var (
	// todo: ormCodespace ok or do we need to claim error codes somehow?
	ErrNotFound         = errors.Register(ormCodespace, 100, "not found")
	ErrIteratorDone     = errors.Register(ormCodespace, 101, "iterator done")
	ErrIteratorInvalid  = errors.Register(ormCodespace, 102, "iterator invalid")
	ErrType             = errors.Register(ormCodespace, 110, "invalid type")
	ErrUniqueConstraint = errors.Register(ormCodespace, 111, "unique constraint violation")
	ErrArgument         = errors.Register(ormCodespace, 112, "invalid argument")
)

type HasKVStore interface {
	KVStore(key sdk.StoreKey) sdk.KVStore
}

// Index allows efficient prefix scans is stored as key = concat(indexKeyBytes, rowIDUint64) with value empty
// so that the row NaturalKey is allows a fixed with 8 byte integer. This allows the MultiKeyIndex key bytes to be
// variable length and scanned iteratively. The
type Index interface {
	Has(ctx HasKVStore, key []byte) bool
	Get(ctx HasKVStore, key []byte) (Iterator, error)
	PrefixScan(ctx HasKVStore, start []byte, end []byte) (Iterator, error)
	ReversePrefixScan(ctx HasKVStore, start []byte, end []byte) (Iterator, error)
}

// Iterator allows iteration through a sequence of key value pairs
type Iterator interface {
	// LoadNext loads the next value in the sequence into the pointer passed as dest and returns the key. If there
	// are no more items an error is returned
	// The key is the rowID and not any MultiKeyIndex key.
	LoadNext(dest interface{}) (key []byte, err error)
	// Close releases the iterator and should be called at the end of iteration
	io.Closer
}

// RowGetter loads a persistent object by row ID into the destination object. The key returned is the serialized row ID.
type RowGetter func(ctx HasKVStore, rowId uint64, dest interface{}) (key []byte, err error)

// AfterSaveInterceptor defines a callback function to be called on Create + Update.
type AfterSaveInterceptor = func(ctx HasKVStore, rowId uint64, newValue, oldValue interface{}) error

// AfterDeleteInterceptor defines a callback function to be called on Delete operations.
type AfterDeleteInterceptor = func(ctx HasKVStore, rowId uint64, value interface{}) error

// TableBuilder are used to setup new table types.
// This interface provides a set of functions that can be called by indexes.
type TableBuilder interface {
	StoreKey() sdk.StoreKey
	RowGetter() RowGetter
	AddAfterSaveInterceptor(interceptor AfterSaveInterceptor)
	AddAfterDeleteInterceptor(interceptor AfterDeleteInterceptor)
}

func NewTypeSafeRowGetter(storeKey sdk.StoreKey, prefixKey byte, cdc *codec.Codec, model reflect.Type) RowGetter {
	return func(ctx HasKVStore, rowId uint64, dest interface{}) ([]byte, error) {
		if err := assertCorrectType(model, dest); err != nil {
			return nil, err
		}
		store := prefix.NewStore(ctx.KVStore(storeKey), []byte{prefixKey})
		key := EncodeSequence(rowId)
		val := store.Get(key)
		if val == nil {
			return nil, ErrNotFound // todo: discuss how to handle this scenario if we drop error return parameter
		}
		return key, cdc.UnmarshalBinaryBare(val, dest)
	}
}

func assertCorrectType(model reflect.Type, obj interface{}) error {
	tp := reflect.TypeOf(obj)
	if tp.Kind() != reflect.Ptr {
		return errors.Wrap(ErrType, "model destination must be a pointer")
	}
	if model != tp.Elem() {
		return errors.Wrapf(ErrType, "can not use %T with this bucket", obj)
	}
	return nil
}
