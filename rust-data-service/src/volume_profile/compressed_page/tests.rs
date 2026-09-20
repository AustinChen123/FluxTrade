use super::*;

fn limits() -> Limits {
    Limits::new(4096, 1024).unwrap()
}
fn fixture(names: &[&str], method: CompressionMethod) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    for name in names {
        writer
            .start_file(*name, FileOptions::default().compression_method(method))
            .unwrap();
        writer.write_all(b"payload").unwrap();
    }
    writer.finish().unwrap().into_inner()
}
fn central(bytes: &[u8]) -> usize {
    bytes.windows(4).position(|b| b == b"PK\x01\x02").unwrap()
}

#[test]
fn roundtrip_and_exact_limits() {
    for raw in [b"".as_slice(), b"[]", b"payload", &[b'x'; 1024]] {
        let archive = encode(raw, limits()).unwrap();
        let exact = Limits::new(archive.len(), raw.len().max(1)).unwrap();
        assert_eq!(decode(&archive, exact).unwrap(), raw);
        assert_eq!(encode(raw, exact).unwrap(), archive);
        let small_archive = Limits::new(archive.len() - 1, 1024).unwrap();
        assert!(decode(&archive, small_archive).is_err());
        assert!(encode(raw, small_archive).is_err());
    }
    assert!(encode(&[0; 1025], limits()).is_err());
    let archive = encode(b"payload", limits()).unwrap();
    assert!(decode(&archive, Limits::new(4096, 6).unwrap()).is_err());
    for (a, r) in [(0, 1), (1, 0), (1, usize::MAX)] {
        assert!(Limits::new(a, r).is_err());
    }
}

#[test]
fn member_count_name_type_method_matrix() {
    for names in [
        vec![],
        vec![MEMBER, "extra"],
        vec![MEMBER, MEMBER],
        vec!["../response.json"],
        vec!["/response.json"],
        vec!["response.json/"],
        vec!["other"],
    ] {
        assert!(decode(&fixture(&names, CompressionMethod::Deflated), limits()).is_err());
    }
    assert!(decode(&fixture(&[MEMBER], CompressionMethod::Stored), limits()).is_err());
    let mut archive = fixture(&[MEMBER], CompressionMethod::Deflated);
    let offset = central(&archive);
    // Central header: UNIX origin plus symlink external attributes.
    archive[offset + 5] = 3;
    archive[offset + 38..offset + 42].copy_from_slice(&(0o120777_u32 << 16).to_le_bytes());
    assert!(decode(&archive, limits()).is_err());
}

#[test]
fn malformed_truncated_crc_and_size_lies_are_rejected() {
    let archive = encode(b"payload", limits()).unwrap();
    for end in 0..archive.len() {
        assert!(
            decode(&archive[..end], limits()).is_err(),
            "truncated at {end}"
        );
    }
    assert!(decode(b"not a ZIP", limits()).is_err());
    let offset = central(&archive);
    let mut bad_crc = archive.clone();
    bad_crc[offset + 16] ^= 1;
    assert!(decode(&bad_crc, Limits::new(4096, 7).unwrap()).is_err());
    for size in [0_u32, 6, 8, u32::MAX] {
        let mut bad_size = archive.clone();
        bad_size[offset + 24..offset + 28].copy_from_slice(&size.to_le_bytes());
        assert!(decode(&bad_size, limits()).is_err());
    }
    let mut encrypted = archive.clone();
    encrypted[offset + 8] |= 1;
    assert!(decode(&encrypted, limits()).is_err());
    let mut corrupt_deflate = archive;
    let data_start = 30 + MEMBER.len();
    corrupt_deflate[data_start] ^= 0xff;
    assert!(decode(&corrupt_deflate, limits()).is_err());
}

#[test]
fn decompression_hard_limit_ignores_false_small_metadata() {
    let raw = vec![b'x'; 1024];
    let mut archive = encode(&raw, limits()).unwrap();
    let offset = central(&archive);
    archive[offset + 24..offset + 28].copy_from_slice(&1_u32.to_le_bytes());
    assert!(decode(&archive, Limits::new(4096, 8).unwrap()).is_err());
}

#[test]
fn over_limit_encodes_complete_finalization_repeatedly() {
    let before = FINISHES.with(|n| n.get());
    assert!(Limits::new(4096, u32::MAX as usize).is_err());
    for limit in 1..=32 {
        assert!(encode(b"payload", Limits::new(limit, 7).unwrap()).is_err());
    }
    assert_eq!(FINISHES.with(|n| n.get()), before + 32);
    let mut bytes = [0; 2];
    let mut sink = Bounded {
        cursor: Cursor::new(&mut bytes),
        overflow: false,
    };
    sink.seek(SeekFrom::Current(-1)).unwrap();
    assert!(sink.overflow);
    sink.seek(SeekFrom::Start(u64::MAX)).unwrap();
    sink.write_all(b"overflow").unwrap();
    assert_eq!(sink.cursor.get_ref().len(), 2);
}

#[test]
fn eocd_count_and_comment_candidates_fail_before_library_allocation() {
    let archive = encode(b"payload", limits()).unwrap();
    let at = archive.len() - 22;
    for count in [0_u16, 2, 32000, u16::MAX] {
        let mut bad = archive.clone();
        bad[at + 8..at + 10].copy_from_slice(&count.to_le_bytes());
        bad[at + 10..at + 12].copy_from_slice(&count.to_le_bytes());
        assert!(preflight(&bad).is_err());
    }
    for offset in [4, 6, 12, 16, 20] {
        let mut bad = archive.clone();
        bad[at + offset] ^= 1;
        assert!(preflight(&bad).is_err());
    }
    let mut commented = archive.clone();
    commented[at + 20..at + 22].copy_from_slice(&24_u16.to_le_bytes());
    commented.extend_from_slice(&[b'x'; 24]);
    assert_eq!(decode(&commented, limits()).unwrap(), b"payload");
    commented[at + 22..at + 26].copy_from_slice(b"PK\x05\x06");
    assert!(preflight(&commented).is_err());
    commented.truncate(at + 23);
    assert!(preflight(&commented).is_err());
}
