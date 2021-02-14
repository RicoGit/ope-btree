### Engine for searching over encrypted data.

**This is not a working version! It's under rapid development!**

related paper: https://eprint.iacr.org/2013/129.pdf


### Before git push execute:

    cargo fix --allow-dirty --allow-staged
    
    cargo fmt & cargo test
    
    cargo clippy (recomended)
    
### Notes: 

- BTree operarates only bytes, there is no needed to different K and V
- Only tree client now what is actually type of K and V


todo consider use channels instead of callbacks