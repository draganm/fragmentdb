package transaction

func (t *ReadTransaction) Exists(path string) (bool, error) {
	_, err := t.GetKey(path)
	if err == ErrNotExists {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}
