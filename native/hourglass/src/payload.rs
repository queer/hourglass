use rustler::{Binary, OwnedBinary};

/// Copy `bytes` into a new BEAM binary owned by `env`.
///
/// Returns `Err` only if the allocator fails to provide the backing storage
/// (extremely rare; treated as an unknown error at the NIF boundary).
pub fn encode_to_binary<'a>(env: rustler::Env<'a>, bytes: Vec<u8>) -> Result<Binary<'a>, String> {
    let mut owned =
        OwnedBinary::new(bytes.len()).ok_or_else(|| "OwnedBinary::new failed".to_string())?;
    owned.as_mut_slice().copy_from_slice(&bytes);
    Ok(Binary::from_owned(owned, env))
}
