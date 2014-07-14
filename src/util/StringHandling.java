package util;

public class StringHandling {
	/**
	 * Extract file name from a file path.
	 * @param filePath String The file path.
	 * @return String File name of this path.
	 */
	public static String getFileNameFromPath(String filePath) {
		String[] temp = filePath.trim().split("/");
		return temp[temp.length - 1];
	}
}
