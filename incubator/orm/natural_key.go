package orm

import (
	"github.com/cosmos/cosmos-sdk/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

var _ TableBuilder = &naturalKeyTableBuilder{}

type naturalKeyer func(val interface{}) []byte // todo: note: in the api design this does not return an error unlike other indexer functions do

func NewNaturalKeyTableBuilder(prefixData, prefixSeq, prefixIndex byte, key sdk.StoreKey, cdc *codec.Codec, model interface{}, getPrimaryKey naturalKeyer) *naturalKeyTableBuilder {
	if prefixIndex == prefixData || prefixData == prefixSeq {
		panic("prefixIndex must be unique")
	}

	builder := NewAutoUInt64TableBuilder(prefixData, prefixSeq, key, cdc, model)

	idx := NewUniqueIndex(builder, prefixIndex, func(value interface{}) (bytes [][]byte, err error) {
		return [][]byte{getPrimaryKey(value)}, nil
	})
	return &naturalKeyTableBuilder{
		naturalKeyIndex:        idx,
		AutoUInt64TableBuilder: builder,
	}
}

type naturalKeyTableBuilder struct {
	*AutoUInt64TableBuilder
	naturalKeyIndex *UniqueIndex
}

func (a naturalKeyTableBuilder) Build() NaturalKeyTable {
	return NaturalKeyTable{
		autoTable:       a.AutoUInt64TableBuilder.Build(),
		naturalKeyIndex: a.naturalKeyIndex,
	}
}

type NaturalKeyed interface {
	// NaturalKey return immutable and serialized natural key of this object
	NaturalKey() []byte
}

type NaturalKeyTable struct {
	autoTable       AutoUInt64Table
	naturalKeyIndex *UniqueIndex
}

func (a NaturalKeyTable) GetOne(ctx HasKVStore, primKey []byte, dest interface{}) ([]byte, error) {
	it, err := a.Get(ctx, primKey)
	if err != nil {
		return nil, err
	}
	return First(it, dest)
}

func (a NaturalKeyTable) Create(ctx HasKVStore, obj NaturalKeyed) error {
	_, err := a.autoTable.Create(ctx, obj)
	return err
}

func (a NaturalKeyTable) Save(ctx HasKVStore, newValue NaturalKeyed) error {
	rowID, err := a.naturalKeyIndex.RowID(ctx, newValue.NaturalKey())
	if err != nil {
		return err
	}
	return a.autoTable.Save(ctx, rowID, newValue)
}

func (a NaturalKeyTable) Delete(ctx HasKVStore, obj NaturalKeyed) error {
	rowID, err := a.naturalKeyIndex.RowID(ctx, obj.NaturalKey())
	if err != nil {
		return err
	}
	return a.autoTable.Delete(ctx, rowID)
}

// todo: there is no error result as store would panic
func (a NaturalKeyTable) Has(ctx HasKVStore, primKey []byte) (bool, error) {
	rowID, err := a.naturalKeyIndex.RowID(ctx, primKey)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return a.autoTable.Has(ctx, rowID)
}

func (a NaturalKeyTable) Get(ctx HasKVStore, primKey []byte) (Iterator, error) {
	rowID, err := a.naturalKeyIndex.RowID(ctx, primKey)
	if err != nil {
		return nil, err
	}
	return a.autoTable.Get(ctx, rowID)
}

func (a NaturalKeyTable) PrefixScan(ctx HasKVStore, start, end []byte) (Iterator, error) {
	return a.naturalKeyIndex.PrefixScan(ctx, start, end)
}

func (a NaturalKeyTable) ReversePrefixScan(ctx HasKVStore, start, end []byte) (Iterator, error) {
	return a.naturalKeyIndex.ReversePrefixScan(ctx, start, end)
}
