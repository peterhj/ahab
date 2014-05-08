#![crate_id = "ahab#0.1"]
#![crate_type = "lib"]

#![allow(visible_private_types)]
#![feature(default_type_params)]
#![feature(macro_rules)]

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variable)]

extern crate collections;
extern crate serialize;
extern crate sync;

pub mod client;
pub mod server;

mod internal;
