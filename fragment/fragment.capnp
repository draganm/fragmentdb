using Go = import "/go.capnp";

@0xc50de5992c3b327a;

$Go.package("fragment");
$Go.import("fragment");

struct Fragment {
    children @0 :List(Data);
    specific :union {
        dataLeaf @1 :Data;
        dataNode @2 :UInt64;
        trieNode @3 :Data;
        wbbtreeNode @4 :WBBTreeNode;
    }
}

struct WBBTreeNode {
    key @0 :Data;
    countLeft @1 :UInt64;
    countRight @2 :UInt64;
}

