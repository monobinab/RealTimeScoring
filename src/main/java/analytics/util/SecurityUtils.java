package analytics.util;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class SecurityUtils {
	static SecretKeySpec signingKey;
	static Mac mac;
	static {

		signingKey = new SecretKeySpec("mykey".getBytes(), "HmacSHA1");
		try {
			mac = Mac.getInstance("HmacSHA1");
		} catch (NoSuchAlgorithmException e1) {
			e1.printStackTrace();
		}
		try {
			mac.init(signingKey);
		} catch (InvalidKeyException e) {
			e.printStackTrace();
		}

	}

	/*public static String hashLoyaltyId(String l_id) {
		String hashed = new String();
		byte[] rawHmac = mac.doFinal(l_id.getBytes());
		hashed = new String(Base64.encodeBase64(rawHmac));
		return hashed;
	}*/
	

	public static String hashLoyaltyId(String l_id) {
		String hashed = new String();
		try {
			SecretKeySpec signingKey = new SecretKeySpec("mykey".getBytes(), "HmacSHA1");
			Mac mac = Mac.getInstance("HmacSHA1");
			try {
				mac.init(signingKey);
			} catch (InvalidKeyException e) {
				e.printStackTrace();
			}
			byte[] rawHmac = mac.doFinal(l_id.getBytes());
			hashed = new String(Base64.encodeBase64(rawHmac));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return hashed;
	}
}
