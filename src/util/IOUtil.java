package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;

/**
 * 
 * This class is a utility class for DFS I/O. This class provide the following
 * several methods : writeFile(String s), readFile(String filename),
 * writeBinary(byte[] bytes), readBinary(String filename), writeObject(Object
 * object), readConfig(String path)
 * 
 * @author menglonghe
 * @author sidilin
 */
public class IOUtil {
	/**
	 * This method is used to write the contents into the specific file
	 * 
	 * @param content
	 *            String the contents we want to write into the file
	 * @param filename
	 *            String the file name
	 */
	public static void writeToFile(String content, String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
			fos.write(content.getBytes(), 0, content.getBytes().length);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method is used to write the byte array to a specific file
	 * 
	 * @param content
	 *            byte[] it is the contents you want to write
	 * @param filename
	 *            String it is the file name you want to write
	 */
	public static void writeBinary(byte[] content, String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);
			fos.write(content);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method is used to write the object to the specific file
	 * 
	 * @param obj
	 *            Object the object you want to write to the file
	 * @param filename
	 *            String the file name you need to write
	 */
	public static void writeObject(Object obj, String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		FileOutputStream fos = null;
		ObjectOutputStream oos = null;
		try {
			fos = new FileOutputStream(file);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(obj);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fos.close();
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * This method is used to write the checkpoint file to the specific file
	 * 
	 * @param filename
	 *            String the file name you want to write
	 * @param obj
	 *            Object the object you want to write
	 */
	public static void writeCheckpointFiles(String filename, Object obj) {
		synchronized (obj) {
			writeObject(obj, filename);
		}
	}

	/**
	 * This method is used to read the specific file and return the byte array
	 * 
	 * @param filename
	 *            String The file you need to read
	 * @return byte[] The return type is byte array
	 */
	public static byte[] readFile(String filename) {
		File file = new File(filename);
		if (!file.exists()) {
			System.err.println("File " + filename + " does not exist!");
			return null;
		}
		FileInputStream fis = null;
		byte[] content = new byte[(int) file.length()];
		try {
			fis = new FileInputStream(file);
			fis.read(content);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return content;
	}

	/**
	 * This method is used to read an object from the specific file
	 * 
	 * @param filename
	 *            The file name you want to read
	 * @return Object return type is an Object
	 */
	public static Object readObject(String filename) {
		Object content = null;
		File file = new File(filename);
		if (!file.exists()) {
			System.err.println("File " + filename + " does not exist!");
			return null;
		}
		FileInputStream fis = null;
		ObjectInputStream ois = null;
		try {
			fis = new FileInputStream(file);
			ois = new ObjectInputStream(fis);
			content = ois.readObject();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				fis.close();
				ois.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return content;
	}

	/**
	 * This method is used to read the configuration file and fill the values
	 * into the correlated field of the Object
	 * 
	 * @param filename
	 *            the configuration file name you want to read
	 * @param obj
	 *            the Object you want to fill
	 */
	public static void readConf(String filename, Object obj) {
		String content = null;
		try {
			content = new String(readFile(filename), "UTF-8");
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		String lines[] = content.split("\n");
		for (String line : lines) {
			String temp[] = new String[2];
			int position = line.lastIndexOf("=");
			if (position == -1) {
				continue;
			}
			temp[0] = line.substring(0, position);
			temp[1] = line.substring(position + 1, line.length());
			try {
				Field field = obj.getClass().getDeclaredField(temp[0]);
				field.setAccessible(true);
				if (field.getType().isPrimitive()) {
					field.setInt(obj, Integer.parseInt(temp[1]));
				} else if (field.getType().equals(String.class)) {
					field.set(obj, temp[1]);
				} else if (field.getType().equals(Integer.class)) {
					field.set(obj, Integer.parseInt(temp[1]));
				}
			} catch (NoSuchFieldException e) {
				e.printStackTrace();
			} catch (SecurityException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static byte[] readChunk(RandomAccessFile file, long startPosition, int size) throws IOException {
		byte tmp = -1;
		byte[] chunk = new byte[size];
		int index = 0;
		while (index != size && (tmp = file.readByte()) != -1) {
			chunk[index++] = tmp;
		}
		return chunk;
	}
}
