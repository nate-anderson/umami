package umami

type MyKey struct{}

func (mk MyKey) Partition() string {
	return "boondocks"
}

func (mk MyKey) Sort() *int {
	i := 0
	return &i
}

type MyDocument struct {
	ID int64
}

func (md MyDocument) AttributeNames() []string {
	return []string{"id"}
}

func (md MyDocument) Key() (string, *int) {
	return "", Pointer(0)
}

/*
func main() {
	r := New[MyDocument, string, int]()
	myDoc, err := r.Get("stinky", Pointer(0))
	if err != nil {
		panic(err)
	}
	fmt.Println(myDoc)
}
*/
