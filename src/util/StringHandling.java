package util;

public class StringHandling {
	public static String getFileNameFromPath(String filePath) {
		String[] temp = filePath.trim().split("/");
		return temp[temp.length - 1];
	}
}
