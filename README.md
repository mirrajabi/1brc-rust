# One Billion Row Challenge in Rust

This is my attempt at the 1BRC. All human-written and very heavily inspired by other Rust implementations.

## Benchmarks

On a Macbook Pro M2 (2023) this takes around 13s but it is 10x faster on Linux on a modern x86 CPU (~1-2s), possibly due to [macOS not being so great with memory mapping](https://stackoverflow.com/a/5837676/5626646).

## How to run it

If you just want to run it: `make just-run`

If you want to have control, have a look at the Makefile.
