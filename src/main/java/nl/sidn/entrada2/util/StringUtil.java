package nl.sidn.entrada2.util;

public class StringUtil {
    
    /** Compares two strings for equality, ignoring case for ASCII characters only. */
    public static boolean equalsIgnoreCaseAscii(String a, String b) {
        if (a == b) return true;
        if (a == null || b == null || a.length() != b.length()) return false;

        for (int i = 0; i < a.length(); i++) {
            char x = a.charAt(i);
            char y = b.charAt(i);
            if (x == y) continue;

            // ASCII fold
            if ((x | 32) != (y | 32)) return false;
            if (x < 'A' || x > 'Z') return false;
        }
        return true;
}


}
