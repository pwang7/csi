//! Avoid trivial numeric cast

use std::convert::TryFrom;

/// A type cast trait used to replace as conversion.
pub trait Cast {
    /// Performs the conversion.
    fn cast<T>(self) -> T
    where
        T: TryFrom<Self>,
        Self: Sized + std::fmt::Display + Copy,
    {
        T::try_from(self).unwrap_or_else(|_| {
            panic!(
                "Failed to convert from {}: {} to {}",
                std::any::type_name::<Self>(),
                self,
                std::any::type_name::<T>(),
            )
        })
    }
}

impl<U> Cast for U {}
