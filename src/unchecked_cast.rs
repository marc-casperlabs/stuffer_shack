use std::mem;

use generic_array::{ArrayLength, GenericArray};
use memmap::MmapMut;

/// Trait indicating that all bit patterns of a value are valid.
pub unsafe trait Pod: Copy {}

unsafe impl Pod for u8 {}
unsafe impl Pod for u16 {}
unsafe impl Pod for u32 {}
unsafe impl Pod for u64 {}
unsafe impl Pod for u128 {}

unsafe impl Pod for i8 {}
unsafe impl Pod for i16 {}
unsafe impl Pod for i32 {}
unsafe impl Pod for i64 {}
unsafe impl Pod for i128 {}

unsafe impl<T, N> Pod for GenericArray<T, N>
where
    T: Pod,
    N: ArrayLength<T>,
    N::ArrayType: Copy,
{
}

/// Direct immutable access trait for unbounded memory regions.
pub trait UncheckedCast {
    /// Interpret a memory location at the given offset as type `T`.
    fn at<T>(&self, offset: usize) -> &T
    where
        T: Pod;
}

/// Direct mutable access trait for unbounded memory regions.
pub trait UncheckedCastMut {
    /// Interpret a memory location at the given offset as type `T`.
    fn at_mut<T>(&mut self, offset: usize) -> &mut T
    where
        T: Pod;
}

impl UncheckedCast for MmapMut {
    #[inline(always)]
    fn at<T>(&self, offset: usize) -> &T
    where
        T: Pod,
    {
        let slice = &self[offset..(offset + mem::size_of::<T>())];

        let item_ptr = slice.as_ptr() as *const T;
        let item = unsafe { &*item_ptr };

        item
    }
}

impl UncheckedCastMut for MmapMut {
    #[inline(always)]
    fn at_mut<T>(&mut self, offset: usize) -> &mut T
    where
        T: Pod,
    {
        let slice = &mut self[offset..(offset + mem::size_of::<T>())];

        let item_ptr = slice.as_mut_ptr() as *mut T;
        let item = unsafe { &mut *item_ptr };

        item
    }
}
