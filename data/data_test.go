package data_test

import (
	"crypto/rand"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/draganm/fragmentdb/data"
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
)

func TestStore(t *testing.T) {
	t.Run("data has same length as max fragment size", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		f, err := st.Get(k)
		require.False(t, f.HasChildren(), "fragment should not have children")
		d, err := f.Specific().DataLeaf()
		require.NoError(t, err)

		require.Equal(t, []byte{1, 2, 3}, d)

	})

	t.Run("data is one byte longer than max fragment size", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		dw := data.NewDataWriter(st, 3, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		require.NoError(t, err)

		f, err := st.Get(k)

		require.Equal(t, fragment.Fragment_specific_Which_dataNode, f.Specific().Which(), "should be a data node fragment")

		count := f.Specific().DataNode()
		require.NoError(t, err)

		require.Equal(t, uint64(4), count, "data node should record total size of 4")

		require.True(t, f.HasChildren(), "fragment should have children")

		ch, err := f.Children()
		require.NoError(t, err)

		require.Equal(t, 2, ch.Len(), "should have two children")

		t.Run("first child should have first 3 bytes", func(t *testing.T) {
			ck, err := ch.At(0)
			require.NoError(t, err)

			fck := store.BytesToKey(ck)

			cf, err := st.Get(fck)
			require.NoError(t, err)

			d, err := cf.Specific().DataLeaf()
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3}, d)

		})

		t.Run("second child should have last byte", func(t *testing.T) {
			ck, err := ch.At(1)
			require.NoError(t, err)

			fck := store.BytesToKey(ck)

			cf, err := st.Get(fck)
			require.NoError(t, err)

			d, err := cf.Specific().DataLeaf()
			require.NoError(t, err)

			require.Equal(t, []byte{4}, d)

		})

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("data size requires two levels of indirection", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		dw := data.NewDataWriter(st, 1, 2)

		_, err := dw.Write([]byte{1, 2, 3, 4})

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		f, err := st.Get(k)

		require.Equal(t, fragment.Fragment_specific_Which_dataNode, f.Specific().Which(), "should be a data node fragment")

		size := f.Specific().DataNode()
		require.NoError(t, err)

		require.Equal(t, uint64(4), size, "data node should record total size of 4")

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 3, 4}, d)
		})

	})

	t.Run("reading and writing empty data", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		dw := data.NewDataWriter(st, 5, 2)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, 0, len(d))

		})

	})

	t.Run("reading and writing large amount of data", func(t *testing.T) {
		st := fragment.NewStore(store.NewMemoryBackendFactory())

		dw := data.NewDataWriter(st, 5, 2)

		dataSize := 8193

		randomData := make([]byte, dataSize)

		n, err := rand.Read(randomData)
		require.NoError(t, err)
		require.Equal(t, dataSize, n)

		n, err = dw.Write(randomData)

		require.Equal(t, dataSize, n)

		require.NoError(t, err)

		k, err := dw.Finish()
		require.NoError(t, err)

		t.Run("reading data should return original data", func(t *testing.T) {
			r, err := data.NewReader(k, st)
			require.NoError(t, err)

			d, err := ioutil.ReadAll(r)
			require.NoError(t, err)

			require.Equal(t, dataSize, len(d))

			require.Equal(t, randomData, d)
		})

	})

}
