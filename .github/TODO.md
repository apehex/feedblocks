# TODO

## HackTheBlock

## Decompiler

### Data collection

- [ ] solidity source code
- [ ] save as binary in parquet
- [ ] schema:
    - [ ] same as cryo contracts
    - [ ] add columns:
        - [ ] source code
        - [ ] creation assembly
        - [ ] runtime assembly

### Data augmentation

- [ ] compile off chain sources:
    - [ ] OZ library on Github

### Preprocessing

- [x] check that the compilation of sources matches the creation bytecode
- [x] disassemble:
    - [x] creation binary code
    - [x] runtime binary code
    - [x] store in the database
- [ ] normalize:
    - [ ] numbers as 32-byte binary word
    - [ ] with / without comments?
    - [ ] spaces / newlines
    - [ ] identifiers?
- [ ] compile:
    - [x] recreate the source project structure from the data saved in parquet

### Tokenization

- [ ] naive BPE
- [ ] numbers:
    - [ ] from:
        - [ ] scientific notation
        - [ ] fixed point notation
        - [ ] integer format
        - [ ] HEX format
    - [ ] to 256 dim vector of bools = 1 byte = uint8
- [ ] assembly:
    - [ ] keywords
    - [ ] arguments: HEX encoded addresses, values, etc
- [ ] solidity:
    - [ ] keywords
    - [ ] identifiers / variables
    - [ ] indentation
- [ ] encode / embed the **compiler version & args**

### VAE

- [ ] assembly
