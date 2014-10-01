package analytics.util;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.util.dao.VariableDao;

public class SecurityUtils {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(SecurityUtils.class);
	static SecretKeySpec signingKey;
	static Mac mac;
	static {

		signingKey = new SecretKeySpec("mykey".getBytes(), "HmacSHA1");
		try {
			mac = Mac.getInstance("HmacSHA1");
		} catch (NoSuchAlgorithmException e1) {
			LOGGER.error("Unable to create encryption key",e1);
		}
		try {
			mac.init(signingKey);
		} catch (InvalidKeyException e) {
			LOGGER.error("Unable to init encryption key",e);
		}

	}

	public static String hashLoyaltyId(String l_id) {
		String hashed = new String();
		synchronized (mac) {
			if (mac != null) {
				byte[] rawHmac = mac.doFinal(l_id.getBytes());
				hashed = new String(Base64.encodeBase64(rawHmac));
			}
		}
		return hashed;
	}
}
