package kafkablocks.utils;

import org.apache.commons.validator.routines.UrlValidator;
import org.springframework.util.Assert;

public final class ValidationUtils {
    private ValidationUtils() {
    }

    public static boolean isValidUrl(String url) {
        return UrlValidator.getInstance().isValid(url);
    }

    public static void assertUrlParam(String url, String paramName) {
        boolean valid = ValidationUtils.isValidUrl(url);
        Assert.isTrue(valid, String.format("'%s' contains not valid URL: %s", paramName, url));
    }
}
