package wbbtree

import (
	"github.com/draganm/fragmentdb/fragment"
	"github.com/draganm/fragmentdb/store"
	"github.com/pkg/errors"
	capnp "zombiezen.com/go/capnproto2"
)

type nodeModifier struct {
	f fragment.Fragment
	e error
}

func (m *nodeModifier) setError(err error) *nodeModifier {
	m.e = err
	return m
}

func newNodeModifier(f fragment.Fragment) *nodeModifier {
	nm := &nodeModifier{
		f: f,
		e: nil,
	}

	wbtn, err := fragment.NewWBBTreeNode(f.Segment())
	if err != nil {
		return nm.setError(errors.Wrap(err, "while creating new WBBTreeNode"))
	}

	wbtn.SetCountLeft(0)
	wbtn.SetCountRight(0)

	err = f.Specific().SetWbbtreeNode(wbtn)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while setting WBBTreeNode to Fragment"))
	}

	dl, err := capnp.NewDataList(f.Segment(), 3)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while creating new data list"))
	}

	err = f.SetChildren(dl)
	if err != nil {
		return nm.setError(errors.Wrap(err, "while setting children of a WBBTreeNode Fragment"))
	}

	return nm
}

func (n *nodeModifier) err() error {
	return n.e
}

func (n *nodeModifier) setKey(k []byte) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	err = tn.SetKey(k)
	if err != nil {
		n.setError(errors.Wrap(err, "while setting key"))
		return
	}
}

func (n *nodeModifier) setLeftCount(c uint64) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	tn.SetCountLeft(c)
}

func (n *nodeModifier) setRightCount(c uint64) {
	if n.e != nil {
		return
	}

	tn, err := n.f.Specific().WbbtreeNode()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting wbbtreenode"))
		return
	}

	tn.SetCountRight(c)
}

func (n *nodeModifier) setLeftChild(lck store.Key) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	err = ch.Set(0, lck.Bytes())
	if err != nil {
		n.setError(errors.Wrap(err, "while setting left child"))
	}
}

func (n *nodeModifier) setRightChild(rck store.Key) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	err = ch.Set(1, rck.Bytes())
	if err != nil {
		n.setError(errors.Wrap(err, "while setting right child"))
	}
}

func (n *nodeModifier) setValue(vk store.Key) {
	if n.e != nil {
		return
	}

	ch, err := n.f.Children()
	if err != nil {
		n.setError(errors.Wrap(err, "while getting children"))
	}

	err = ch.Set(2, vk.Bytes())
	if err != nil {
		n.setError(errors.Wrap(err, "while setting value"))
	}
}
