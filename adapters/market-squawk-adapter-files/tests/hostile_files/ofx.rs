use super::*;

#[tokio::test]
async fn ofx_and_qfx_enforce_statement_identity_totals_and_unique_transactions()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let valid_sgml = [
        b"OFXHEADER:100\nDATA:OFXSGML\nVERSION:160\nSECURITY:NONE\nENCODING:USASCII\nCHARSET:1252\nCOMPRESSION:NONE\nOLDFILEUID:NONE\nNEWFILEUID:NONE\n\n<OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>USD\n<BANKACCTFROM><BANKID>123456789\n<ACCTID>acct-1\n<ACCTTYPE>CHECKING\n</BANKACCTFROM><BANKTRANLIST><DTSTART>20260701000000\n<DTEND>20260718120000[-4:EDT]\n<STMTTRN><TRNTYPE>DEBIT\n<DTPOSTED>20260718120000[-4:EDT]\n<TRNAMT>-12.50\n<FITID>fit-1\n<NAME>Caf"
            .as_slice(),
        &[0xe9],
        b"\n</STMTTRN></BANKTRANLIST><LEDGERBAL><BALAMT>100.00\n<DTASOF>20260718120000[-4:EDT]\n</LEDGERBAL></STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"
            .as_slice(),
    ]
    .concat();
    for format in ["ofx", "qfx"] {
        let name = format!("valid.{format}");
        fs::write(directory.path().join(&name), &valid_sgml)?;
        assert_eq!(
            extract_fixture(&directory, &name, format)
                .await??
                .records()
                .len(),
            1
        );
    }

    let duplicate_xml = br#"<?xml version="1.0" encoding="UTF-8"?><?OFX OFXHEADER="200" VERSION="230" SECURITY="NONE" OLDFILEUID="NONE" NEWFILEUID="NONE"?><OFX><BANKMSGSRSV1><STMTTRNRS><STMTRS><CURDEF>USD</CURDEF><BANKACCTFROM><BANKID>123456789</BANKID><ACCTID>acct-1</ACCTID><ACCTTYPE>CHECKING</ACCTTYPE></BANKACCTFROM><BANKTRANLIST><DTSTART>20260701000000</DTSTART><DTEND>20260718120000[-4:EDT]</DTEND><STMTTRN><TRNTYPE>DEBIT</TRNTYPE><DTPOSTED>20260718120000[-4:EDT]</DTPOSTED><TRNAMT>-12.50</TRNAMT><FITID>duplicate</FITID></STMTTRN><STMTTRN><TRNTYPE>CREDIT</TRNTYPE><DTPOSTED>20260718130000[-4:EDT]</DTPOSTED><TRNAMT>1.00</TRNAMT><FITID>duplicate</FITID></STMTTRN></BANKTRANLIST><LEDGERBAL><BALAMT>100.00</BALAMT><DTASOF>20260718130000[-4:EDT]</DTASOF></LEDGERBAL></STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>"#;
    let second_start = duplicate_xml
        .windows(b"<STMTTRN><TRNTYPE>CREDIT".len())
        .position(|window| window == b"<STMTTRN><TRNTYPE>CREDIT")
        .ok_or("second XML OFX transaction is absent")?;
    let second_end_relative = duplicate_xml
        .get(second_start..)
        .ok_or("second XML OFX transaction start is invalid")?
        .windows(b"</STMTTRN>".len())
        .position(|window| window == b"</STMTTRN>")
        .ok_or("second XML OFX transaction end is absent")?;
    let second_end = second_start
        .checked_add(second_end_relative)
        .and_then(|position| position.checked_add(b"</STMTTRN>".len()))
        .ok_or("second XML OFX transaction range overflow")?;
    let mut valid_xml = duplicate_xml
        .get(..second_start)
        .ok_or("valid XML OFX prefix is absent")?
        .to_vec();
    valid_xml.extend_from_slice(
        duplicate_xml
            .get(second_end..)
            .ok_or("valid XML OFX suffix is absent")?,
    );
    for (name, format, base, suffix, accepted) in [
        (
            "sgml-space.ofx",
            "ofx",
            valid_sgml.as_slice(),
            b"\n \t".as_slice(),
            true,
        ),
        (
            "sgml-junk.ofx",
            "ofx",
            valid_sgml.as_slice(),
            b"junk".as_slice(),
            false,
        ),
        (
            "sgml-root.ofx",
            "ofx",
            valid_sgml.as_slice(),
            b"<OFX></OFX>".as_slice(),
            false,
        ),
        (
            "xml-space.qfx",
            "qfx",
            valid_xml.as_slice(),
            b"\n \t".as_slice(),
            true,
        ),
        (
            "xml-junk.qfx",
            "qfx",
            valid_xml.as_slice(),
            b"junk".as_slice(),
            false,
        ),
        (
            "xml-root.qfx",
            "qfx",
            valid_xml.as_slice(),
            b"<OFX></OFX>".as_slice(),
            false,
        ),
    ] {
        let mut payload = base.to_vec();
        payload.extend_from_slice(suffix);
        fs::write(directory.path().join(name), payload)?;
        let extracted = extract_fixture(&directory, name, format).await?;
        assert_eq!(extracted.is_ok(), accepted, "{name}");
    }
    fs::write(directory.path().join("duplicate.ofx"), duplicate_xml)?;
    let error = extract_fixture(&directory, "duplicate.ofx", "ofx")
        .await?
        .err()
        .ok_or("duplicate OFX transaction unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeOfx);

    let valid_text = String::from_utf8_lossy(&valid_sgml).into_owned();
    for (name, payload) in [
        (
            "investment.ofx",
            valid_text
                .replace("<STMTRS>", "<INVSTMTRS>")
                .replace("</STMTRS>", "</INVSTMTRS>"),
        ),
        (
            "misplaced.ofx",
            valid_text
                .replace("<BANKMSGSRSV1><STMTTRNRS>", "")
                .replace("</STMTTRNRS></BANKMSGSRSV1>", ""),
        ),
    ] {
        fs::write(directory.path().join(name), payload)?;
        let error = extract_fixture(&directory, name, "ofx")
            .await?
            .err()
            .ok_or("unsupported or misplaced OFX statement unexpectedly succeeded")?;
        assert_eq!(error, FileAdapterError::UnsafeOfx, "{name}");
    }

    for (name, needle, replacement) in [
        ("account.ofx", b"acct-1".as_slice(), b"acct-2".as_slice()),
        ("balance.ofx", b"100.00".as_slice(), b"bad.de".as_slice()),
    ] {
        let mut payload = valid_sgml.clone();
        let position = payload
            .windows(needle.len())
            .position(|window| window == needle)
            .ok_or("OFX fixture replacement target is absent")?;
        payload[position..position + needle.len()].copy_from_slice(replacement);
        fs::write(directory.path().join(name), payload)?;
        let error = extract_fixture(&directory, name, "ofx")
            .await?
            .err()
            .ok_or("invalid OFX statement unexpectedly succeeded")?;
        assert_eq!(error, FileAdapterError::UnsafeOfx);
    }
    Ok(())
}
