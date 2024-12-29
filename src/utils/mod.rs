mod qp;
mod timer;
pub mod timing;
mod wait;

pub use qp::*;
pub use timer::*;
pub use wait::*;

/// Indicate the lock is unacquired.
pub const UNACQUIRED: u8 = 0;

/// Indicate the current client has attempted to acquired the lock, but currently cannot.
/// This state means that it is an error to reattempt to acquire the lock from `UNACQUIRED` state.
pub const ACQUIRING: u8 = 1;

/// Indicate the lock is acquired by the current client.
pub const ACQUIRED: u8 = 2;

/// Return the highest significant bit of a number.
pub const fn highbit(n: u64) -> u64 {
    if n == 0 {
        0
    } else {
        1 << (63 - n.leading_zeros())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_highbit() {
        assert_eq!(highbit(0), 0);
        assert_eq!(highbit(1), 1);
        assert_eq!(highbit(0b11010010), 0b10000000);
        assert_eq!(highbit(!0), 1u64 << 63);
    }
}
