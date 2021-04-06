### Engine for searching over encrypted data.

**Get and Put operations are implemented and working fine, but OpeDatabse isn't ready for use**

related paper: https://eprint.iacr.org/2013/129.pdf

### Before git push execute:

    cargo fix --allow-dirty --allow-staged
    
    cargo fmt & cargo test
    
    cargo clippy (recomended)
    
    
### Run test with logs

    RUST_LOG=debug cargo test ope_btree::tests::put_many_and_get_them_back_test -- --nocapture    
    
### Notes: 

- BTree operates only bytes, there is no needed to different K and V
- Only tree client know what is actually type of K and V
- Maybe consider using channels instead of callbacks


### Progress

- [x] create base entities (Node, Cmd, MPath, etc.)
- [x] implement KVStore
- [x] implement get operation
- [x] implement put operation
- [x] integration test for BTree
- [x] create Database
- [x] integration test for Database
- [x] implement transport (Grpc)
- [ ] make integration test with real transport
- [ ] create CI with Github actions
- [ ] make distribution for server (build docker image)
- [ ] make cli client
- [ ] make wasm cli
- [ ] verifying non-trivial rebalancing
- [ ] fix all todos
- [ ] implement range operation (when Streams will be stabilized) 
- [ ] implement delete operation 
- [ ] implement rockDb backend for all KVStores
