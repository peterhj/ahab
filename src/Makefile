ROOT    := $(abspath ..)
include $(ROOT)/config.mk

LIBAHAB := $(shell rustc --crate-file-name libahab/lib.rs)
TEST    := test

.PHONY: all clean

all: $(LIBAHAB) $(TEST)

clean:
	rm -f *.d
	rm -f $(LIBAHAB)
	rm -f $(TEST)

-include $(LIBAHAB).d
$(LIBAHAB): libahab/lib.rs
	$(RUSTC) $(RUST_FLAGS) --dep-info $@.d --out-dir . $<

$(TEST): test.rs $(LIBAHAB)
	$(RUSTC) $(RUST_FLAGS) $<
