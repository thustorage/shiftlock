#![macro_use]

pub(crate) const fn range_mask(range: std::ops::Range<usize>) -> u64 {
    if range.end % 64 == 0 {
        !((1u64 << range.start % 64) - 1)
    } else {
        ((1u64 << range.end % 64) - 1) & !((1u64 << range.start % 64) - 1)
    }
}

pub(crate) fn value_at_mask(value: impl Into<u64>, mask: u64) -> u64 {
    if mask == 0 {
        if cfg!(debug_assertions) {
            panic!("value mask is zero");
        } else {
            unsafe {
                std::hint::unreachable_unchecked();
            }
        }
    }

    let value = value.into();
    let res = value << mask.trailing_zeros();
    debug_assert!(
        res & !mask == 0,
        "value {value} is wider than mask {:#x}",
        mask
    );
    res
}

/// Implement basic methods for lock data structures.
macro_rules! impl_lock_basic_methods {
    ($LockT:ty, $bits:expr) => {
        impl $LockT {
            /// Create a zeroed lock entry.
            pub fn new() -> Self {
                Self(::bitvec::prelude::bitarr![0; $bits])
            }

            /// Interpret the bytes at the given address as a lock entry.
            ///
            /// # Safety
            ///
            /// The address must be valid and properly aligned.
            pub unsafe fn from_addr<'a>(addr: *const u8) -> &'a Self {
                debug_assert!(!addr.is_null());
                debug_assert!(addr as usize % std::mem::align_of::<Self>() == 0);
                &*(addr as *const Self)
            }

            /// Interpret the bytes at the given address as a mutable lock entry.
            ///
            /// # Safety
            ///
            /// The address must be valid and properly aligned.
            pub unsafe fn from_addr_mut<'a>(addr: *mut u8) -> &'a mut Self {
                debug_assert!(!addr.is_null());
                debug_assert!(addr as usize % std::mem::align_of::<Self>() == 0);
                &mut *(addr as *mut Self)
            }

            ::paste::paste! {
                /// Interpret the lock entry as an integer.
                pub fn [<as_u $bits>](&self) -> [<u $bits>] {
                    self.0.load_le()
                }
            }
        }

        impl Default for $LockT {
            fn default() -> Self {
                Self::new()
            }
        }
    }
}

/// Implement setters and getters for lock data structure fields.
/// Must use within an `impl` block.
macro_rules! define_field_accessor {
    ($field:ident, $FieldT:ty, $range:expr) => {
        pub fn $field(self) -> $FieldT {
            self.0[$range].load_le()
        }

        ::paste::paste! {
            pub fn [<set_ $field>](&mut self, value: $FieldT) {
                self.0[$range].store_le(value);
            }
        }
    };

    ($field:ident, $FieldT:ty, $range:expr, WITH_MASK) => {
        pub fn $field(self) -> $FieldT {
            self.0[$range].load_le()
        }

        ::paste::paste! {
            pub fn [<set_ $field>](&mut self, value: $FieldT) {
                self.0[$range].store_le(value);
            }

            #[allow(non_upper_case_globals)]
            pub const [<MASK_ $field>]: u64 = crate::macros::range_mask($range);
        }
    };
}

/// Generate a mask of the specified fields.
macro_rules! mask_of {
    ($LockT:ty: $($field:ident),* $(,)*) => {
        ::paste::paste! {
            $(
                $LockT::[<MASK_ $field>]
            )|*
        }
    };

    (FAA, $LockT:ty: $($field:ident),* $(,)*) => {
        ::paste::paste! {
            $(
                crate::utils::highbit($LockT::[<MASK_ $field>])
            )|*
        }
    };
}

/// Generate a bit representation that puts the specified fields at the given positions.
macro_rules! bit_repr_of {
    ($LockT:ty: { $($field:ident: $val:expr),* $(,)* }) => {
        ::paste::paste! {
            $(
                crate::macros::value_at_mask($val, $LockT::[<MASK_ $field>])
            )|*
        }
    };
}
