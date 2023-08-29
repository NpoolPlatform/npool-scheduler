package asyncfeed

func AsyncFeed(ent interface{}, ch chan interface{}) {
	go func() {
		ch <- ent
	}()
}
