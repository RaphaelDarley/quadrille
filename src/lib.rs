use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::Relaxed;

mod stores;
#[cfg(test)]
mod test;

pub enum QuadrilleError {
    KeyConflict,
}
pub trait KVStore: Default {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn insert(&self, key: Vec<u8>, val: Vec<u8>) -> (Self, bool);
    fn resolve(basis: Arc<Self>, prev: Arc<Self>) -> Result<Arc<Self>, QuadrilleError>;
}

#[derive(Clone)]
pub struct Quadrille<T: KVStore> {
    inner: Arc<AtomicRoot<T>>,
}

struct UnsafeDrop<T: ?Sized> {
    inner: ManuallyDrop<T>,
}

impl<T> UnsafeDrop<T> {
    pub fn new(val: T) -> UnsafeDrop<T> {
        let inner = ManuallyDrop::new(val);
        UnsafeDrop { inner }
    }

    pub unsafe fn drop(self) {
        let _ = ManuallyDrop::into_inner(self.inner);
    }

    pub unsafe fn into_inner(self) -> T {
        ManuallyDrop::into_inner(self.inner)
    }
}

impl<T: ?Sized> Deref for UnsafeDrop<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(Copy)]
struct Basis<T>(*mut T);
impl<T> Basis<T> {
    fn new(ptr: *mut T) -> Basis<T> {
        Basis(ptr)
    }
    fn unwrap(self) -> *mut T {
        self.0
    }
}

impl<T> Clone for Basis<T> {
    fn clone(&self) -> Self {
        Basis(self.0)
    }
}

struct AtomicRoot<T> {
    inner: AtomicPtr<T>,
}

impl<T> AtomicRoot<T> {
    pub fn new(val: T) -> AtomicRoot<T> {
        let arc = Arc::new(val);
        let inner = AtomicPtr::new(Arc::into_raw(arc) as *mut T);
        Self { inner }
    }
    pub fn get(&self) -> Arc<T> {
        let inner = self.get_inner();
        let out = (*inner).clone();
        out
    }

    fn get_inner(&self) -> UnsafeDrop<Arc<T>> {
        // SAFTEY: self.inner is only set as the result of Arc::into_raw, and will not be dropped automatically
        // Dropping must only occur once, and when the Arc pointer is removed from the struct
        let arc = unsafe { Arc::from_raw(self.inner.load(Relaxed)) };
        UnsafeDrop::new(arc)
    }

    pub fn swap(&self, val: Arc<T>) -> Arc<T> {
        let new_ptr = Arc::into_raw(val);
        let old_ptr = self.inner.swap(new_ptr as *mut T, Relaxed);
        unsafe { Arc::from_raw(old_ptr) }
    }

    pub fn basis(&self) -> (Basis<T>, Arc<T>) {
        let inner = self.get_inner();
        let cloned = (*inner).clone();
        let ptr = Arc::into_raw(unsafe { inner.into_inner() }) as *mut T;

        (Basis::new(ptr), cloned)
    }

    pub fn compare_swap(&self, basis: Basis<T>, new: Arc<T>) -> Result<Arc<T>, Arc<T>> {
        let new_ptr = Arc::into_raw(new) as *mut T;
        let old_ptr = basis.unwrap();
        let res = self
            .inner
            .compare_exchange(old_ptr, new_ptr, Relaxed, Relaxed);
        match res {
            Ok(ptr) => Ok(unsafe { Arc::from_raw(ptr) }),
            Err(_) => Err(unsafe { Arc::from_raw(new_ptr) }),
        }
    }
}

impl<T> Drop for AtomicRoot<T> {
    fn drop(&mut self) {
        let inner = self.get_inner();
        unsafe {
            UnsafeDrop::drop(inner);
        }
    }
}

pub struct Transation<T: KVStore> {
    kv: Arc<AtomicRoot<T>>,
    basis_marker: Basis<T>,
    basis: Arc<T>,
    current: Arc<T>,
}

impl<T: KVStore> Transation<T> {
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.current.get(key)
    }

    pub fn insert(&mut self, key: Vec<u8>, val: Vec<u8>) -> bool {
        let (new, found) = self.current.insert(key, val);
        self.current = new.into();
        found
    }

    pub fn commit(mut self) -> Result<Quadrille<T>, QuadrilleError> {
        loop {
            match self
                .kv
                .compare_swap(self.basis_marker.clone(), self.current.clone())
            {
                Ok(_) => {
                    // TODO: drop _ptr
                    break;
                }
                Err(_) => {
                    self.update_basis();
                    self.current = T::resolve(self.basis.clone(), self.current.clone())?;
                }
            }
        }
        Ok(Quadrille { inner: self.kv })
    }

    fn update_basis(&mut self) {
        let (m, b) = self.kv.basis();
        self.basis_marker = m;
        self.basis = b;
    }
}

impl<T: KVStore> Quadrille<T> {
    pub fn transaction(&self) -> Transation<T> {
        let tx_root = self.inner.clone();
        let (basis_marker, basis) = tx_root.basis();
        let current = basis.clone();
        Transation {
            kv: tx_root,
            basis_marker,
            basis,
            current,
        }
    }

    pub fn new() -> Quadrille<T> {
        let root = AtomicRoot::new(T::default());
        let inner = Arc::new(root);
        Quadrille { inner }
    }
}

impl<T: KVStore> Drop for Quadrille<T> {
    fn drop(&mut self) {}
}
