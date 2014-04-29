#[deriving(Clone)]
pub struct HostId(u32);

#[deriving(Clone)]
pub struct AhabConfig {
  identity: HostId,
}

impl AhabConfig {
  pub fn new() -> AhabConfig {
    AhabConfig{
      identity: HostId(0),
    }
  }
}
