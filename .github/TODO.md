# TODO

## HackTheBlock

## Decompiler

### Data collection

- [x] solidity source code
- [x] save as binary in parquet
- [x] schema:
    - [x] same as cryo contracts
    - [ ] add source code

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
    - [x] numbers as 32-byte binary word
    - [ ] with / without comments?
    - [ ] spaces / newlines
    - [ ] identifiers?
- [x] compile:
    - [x] recreate the source project structure from the data saved in parquet

### Tokenization

- [ ] numbers:
    - [ ] from:
        - [ ] scientific notation
        - [ ] fixed point notation
        - [ ] integer format
        - [ ] HEX format
    - [ ] to 256 dim vector of bools = 4 bytes = uint32
- [x] assembly:
    - [x] keywords
    - [x] arguments: HEX encoded addresses, values, etc
- [ ] solidity:
    - [ ] naive BPE
    - [ ] keywords
    - [ ] identifiers / variables
    - [ ] indentation
- [ ] encode / embed the **compiler version & args**

### Models

- [ ] VAEs:
    - [ ] assembly
    - [ ] solidity
