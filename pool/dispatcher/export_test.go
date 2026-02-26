package dispatcher

func (d *Dispatcher) InjectJob(id JobID, nonce string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.History[id] = nonce
}
