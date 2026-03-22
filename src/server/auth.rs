/// MySQL native password authentication.
///
/// The `mysql_native_password` protocol works as follows:
///   token = SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password)))
/// The server stores SHA1(SHA1(password)) (the "double hash").
/// To verify, the server computes SHA1(challenge + stored_double_hash),
/// XORs it with the client token to recover SHA1(password), then hashes
/// that once more and compares with stored_double_hash.

/// Minimal SHA1 implementation (FIPS 180-4) — no external crate needed.
struct Sha1 {
    state: [u32; 5],
    count: u64,
    buffer: [u8; 64],
    buffer_len: usize,
}

impl Sha1 {
    fn new() -> Self {
        Self {
            state: [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0],
            count: 0,
            buffer: [0u8; 64],
            buffer_len: 0,
        }
    }

    fn update(&mut self, data: &[u8]) {
        let mut offset = 0;
        self.count += data.len() as u64;

        if self.buffer_len > 0 {
            let space = 64 - self.buffer_len;
            let copy_len = space.min(data.len());
            self.buffer[self.buffer_len..self.buffer_len + copy_len]
                .copy_from_slice(&data[..copy_len]);
            self.buffer_len += copy_len;
            offset = copy_len;
            if self.buffer_len == 64 {
                let block = self.buffer;
                self.transform(&block);
                self.buffer_len = 0;
            }
        }

        while offset + 64 <= data.len() {
            let mut block = [0u8; 64];
            block.copy_from_slice(&data[offset..offset + 64]);
            self.transform(&block);
            offset += 64;
        }

        if offset < data.len() {
            let remaining = data.len() - offset;
            self.buffer[..remaining].copy_from_slice(&data[offset..]);
            self.buffer_len = remaining;
        }
    }

    fn finalize(mut self) -> [u8; 20] {
        let bit_count = self.count * 8;
        // Padding
        let mut pad = vec![0x80u8];
        let pad_len = if self.buffer_len < 56 {
            55 - self.buffer_len
        } else {
            119 - self.buffer_len
        };
        pad.extend(std::iter::repeat(0u8).take(pad_len));
        pad.extend_from_slice(&bit_count.to_be_bytes());
        self.update(&pad);

        let mut result = [0u8; 20];
        for (i, &s) in self.state.iter().enumerate() {
            result[i * 4..i * 4 + 4].copy_from_slice(&s.to_be_bytes());
        }
        result
    }

    fn transform(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }

        let [mut a, mut b, mut c, mut d, mut e] = self.state;

        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                _ => (b ^ c ^ d, 0xCA62C1D6u32),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }

        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);
    }
}

/// Compute SHA1 hash of data.
pub fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h = Sha1::new();
    h.update(data);
    h.finalize()
}

/// Compute the double-SHA1 hash used for password storage.
/// stored_hash = SHA1(SHA1(password))
pub fn double_sha1(password: &str) -> [u8; 20] {
    let h1 = sha1(password.as_bytes());
    sha1(&h1)
}

/// Validate a mysql_native_password authentication response.
///
/// `challenge` is the 20-byte random challenge sent in the server greeting.
/// `client_response` is the 20-byte token from the client.
/// `stored_double_hash` is SHA1(SHA1(password)) stored on the server.
///
/// Returns `true` if the client knows the correct password.
pub fn validate_native_password(
    challenge: &[u8],
    client_response: &[u8],
    stored_double_hash: &[u8; 20],
) -> bool {
    if client_response.len() != 20 {
        return false;
    }

    // Compute SHA1(challenge + stored_double_hash)
    let mut concat = Vec::with_capacity(challenge.len() + 20);
    concat.extend_from_slice(challenge);
    concat.extend_from_slice(stored_double_hash);
    let hash_stage = sha1(&concat);

    // Recover SHA1(password) by XORing client_response with hash_stage
    let mut recovered = [0u8; 20];
    for i in 0..20 {
        recovered[i] = client_response[i] ^ hash_stage[i];
    }

    // Hash recovered once more and compare with stored_double_hash
    let check = sha1(&recovered);
    check == *stored_double_hash
}

/// Generate a 20-byte challenge from a connection_id and a random seed.
pub fn generate_challenge(connection_id: u32) -> [u8; 20] {
    // Use connection_id mixed with constants to produce a deterministic
    // but unique-per-connection challenge. In production, use a CSPRNG.
    let id = connection_id;
    let mut challenge = [0u8; 20];
    let seed = id.wrapping_mul(0x9E3779B9); // golden ratio hash
    for (i, b) in challenge.iter_mut().enumerate() {
        let mix = seed.wrapping_add(i as u32).wrapping_mul(0x6C078965);
        *b = (mix >> ((i % 4) * 8)) as u8;
    }
    challenge
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_empty() {
        let hash = sha1(b"");
        let expected = [
            0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
            0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
        ];
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_sha1_abc() {
        let hash = sha1(b"abc");
        let expected = [
            0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e,
            0x25, 0x71, 0x78, 0x50, 0xc2, 0x6c, 0x9c, 0xd0, 0xd8, 0x9d,
        ];
        assert_eq!(hash, expected);
    }

    #[test]
    fn test_validate_native_password() {
        let password = "secret";
        let double_hash = double_sha1(password);
        let challenge = generate_challenge(42);

        // Simulate client: token = SHA1(password) XOR SHA1(challenge + double_hash)
        let sha1_password = sha1(password.as_bytes());
        let mut concat = Vec::new();
        concat.extend_from_slice(&challenge);
        concat.extend_from_slice(&double_hash);
        let hash_stage = sha1(&concat);

        let mut token = [0u8; 20];
        for i in 0..20 {
            token[i] = sha1_password[i] ^ hash_stage[i];
        }

        assert!(validate_native_password(&challenge, &token, &double_hash));

        // Wrong token should fail
        let mut bad_token = token;
        bad_token[0] ^= 0xFF;
        assert!(!validate_native_password(&challenge, &bad_token, &double_hash));
    }

    #[test]
    fn test_empty_response_fails() {
        let double_hash = double_sha1("pass");
        let challenge = generate_challenge(1);
        assert!(!validate_native_password(&challenge, &[], &double_hash));
    }
}
