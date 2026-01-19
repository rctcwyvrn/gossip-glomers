#!/usr/bin/env bash
maelstrom test -w unique-ids --bin ./target/release/gossip-glomers --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition