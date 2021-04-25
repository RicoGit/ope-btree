### Engine for searching over encrypted data.

**Get and Put operations are implemented and working fine, but OpeDatabse isn't ready for use**

related paper: https://eprint.iacr.org/2013/129.pdf

### Before git push execute:

    cargo fix --allow-dirty --allow-staged
    
    cargo fmt & cargo test
    
    cargo clippy (recomended)
    
    
### Run test with logs

    RUST_LOG=debug cargo test ope_btree::tests::put_many_and_get_them_back_test -- --nocapture    
    
### Run client and server: 

Run Grpc-based Ope Db server implementation

``bash
    cargo run --bin server-grpc -- --arity 4  
``

Run Grpc-based Ope Db client implementation

``bash
    cargo run --bin client-grpc -- --host http://localhost:7777 --debug-mode true
``

### Progress

- [x] create base entities (Node, Cmd, MPath, etc.)
- [x] implement KVStore
- [x] implement get operation
- [x] implement put operation
- [x] integration test for BTree
- [x] create Database
- [x] integration test for Database
- [x] implement transport (Grpc)
- [x] make integration test with real transport
- [ ] create CI with Github actions
- [x] make distribution for the server (build docker image)
- [ ] make cli client
- [ ] make wasm cli
- [ ] verifying non-trivial rebalancing
- [ ] fix all todos
- [ ] implement range operation (when Streams will be stabilized) 
- [ ] implement delete operation 
- [ ] implement rockDb backend for all KVStores
- [ ] keep min and max elem of tree for quick response if key < min and > max (optimisation)
- [ ] bulk upload api is nice to have.
- [ ] implement OPE table [optional] (OPE Table is updated to map the DET ciphertexts to their new or updated OPE encodings, see paper)
- [ ] Put operation should rewrite encrypted key and value
