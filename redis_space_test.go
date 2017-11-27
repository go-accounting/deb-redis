package debredis

import (
	"fmt"
	"go-accounting/deb"
	"testing"

	"github.com/alicebob/miniredis"
)

type RedisSpaceBuilder int

func (dsb RedisSpaceBuilder) NewSpace(arr deb.Array, metadata [][][]byte) deb.Space {
	return dsb.NewSpaceWithOffset(arr, 0, 0, metadata)
}

func (RedisSpaceBuilder) NewSpaceWithOffset(arr deb.Array, do, mo int, metadata [][][]byte) deb.Space {
	s, err := miniredis.Run()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	space, err := NewRedisSpace("", []string{s.Addr()}, "")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if arr != nil {
		if err := space.Append(deb.NewSmallSpaceWithOffset(arr, uint64(do), uint64(mo),
			metadata)); err != nil {
			fmt.Println(err)
			return nil
		}
	}
	return space
}

func TestRedisSpaceTransactions(t *testing.T) {
	deb.SpaceTester(0).TestTransactions(t, RedisSpaceBuilder(0))
}
func TestRedisSpaceAppend(t *testing.T) {
	deb.SpaceTester(0).TestAppend(t, RedisSpaceBuilder(0))
}
func TestRedisSpaceSlice(t *testing.T) {
	deb.SpaceTester(0).TestSlice(t, RedisSpaceBuilder(0))
}
func TestRedisSpaceProjection(t *testing.T) {
	deb.SpaceTester(0).TestProjection(t, RedisSpaceBuilder(0))
}
