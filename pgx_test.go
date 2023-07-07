package px

import "testing"

func TestToTableName(t *testing.T) {
	s := ToTableName("follow")
	if s != `follows` {
		t.Error("s is not `follows` , but ", s)
		return
	}
}
