package be.unamur.polystore.acceleo.tests;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import be.unamur.polystore.acceleo.main.Generate;

public class TestExecutor {
	private static boolean localExecution = false;
	private static boolean logMvnTest = false;

	private static final String unitTestDirName = "be.unamur.polystore.unittests";
	private static final String dockerDirName = "docker";
	private static final File unitTestDir = new File(
			Paths.get(new File(System.getProperty("user.dir")).getParentFile().getAbsolutePath())
					.resolve(unitTestDirName).toString());
	private static final File dockerDir = new File(
			Paths.get(unitTestDir.getAbsolutePath()).resolve(dockerDirName).toString());
	private static Map<String, File> testFiles = new HashMap<String, File>();

	public static void main(String[] args) throws Exception {
		localExecution = args == null || args.length == 0;
		if(localExecution)
			System.out.println("Local process");
		else
			System.out.println("Jenkins process");
		
		
//		if (localExecution)
//			deployPolystore();

		try {
			System.out.println(unitTestDir.getAbsolutePath());
			getPMLFiles();
			System.out.println(getDate() + "Number of PML files detected: " + testFiles.size());
			System.out.println();
			if (testFiles.size() > 0) {
				System.out.println(getDate() + "DAO Generation:");
				int i = 0;
				for (Entry<String, File> entry : testFiles.entrySet()) {
					generateDAO(entry.getKey(), entry.getValue());
					pasteUnitTests(entry.getKey(), entry.getValue());
					i++;
					System.out.println(getDate() + (i) + "/" + testFiles.size() + ") " + entry.getKey()
							+ ": DAO generated with success.");
				}

				System.out.println();

				if (localExecution) {
					System.out.println(getDate() + "MVN test:");
					i = 0;
					for (Entry<String, File> entry : testFiles.entrySet()) {
						boolean error = startMvnTest(entry.getKey(), entry.getValue());
						i++;
						System.out.println(getDate() + (i) + "/" + testFiles.size() + ") " + entry.getKey() + ": "
								+ (error ? "unit tests encoutered errors" : "unit tests executed with success")
								+ ". Log available: " + entry.getValue().getParentFile().getAbsolutePath()
								+ File.separator + "logs" + File.separator + entry.getKey() + ".log");
					}
				}
			}
		} finally {
//			if (localExecution)
//				stopPolystore();
		}

	}

	private static void deployPolystore() throws Exception {
		Process process = Runtime.getRuntime().exec("docker-compose up -d", null, dockerDir);
		process.waitFor();
		int exit = process.exitValue();
		if (exit != 0)
			throw new Exception("Impossible to start the polystore");
		System.out.println(getDate() + "Polystore started.");
	}

	private static void stopPolystore() throws Exception {
		Process process = Runtime.getRuntime().exec("docker-compose stop", null, dockerDir);
		process.waitFor();
		int exit = process.exitValue();
		if (exit != 0)
			throw new Exception("Impossible to stop the polystore");
		System.out.println(getDate() + "Polystore stopped.");
	}

	private static boolean startMvnTest(String testName, File pml) throws IOException, InterruptedException {

		File testDir = new File(Paths.get(pml.getParentFile().getAbsolutePath()).resolve(testName).toString());
		Process process = Runtime.getRuntime().exec("mvn.cmd clean package", null, testDir);

		new Thread() {
			public void run() {
				DataOutputStream stdin = new DataOutputStream(process.getOutputStream());
				File directory = new File(testDir.getParentFile().getAbsolutePath() + File.separator + "logs");
				if (!directory.exists()) {
					directory.mkdir();
				}

				File fnew = new File(testDir.getParentFile().getAbsolutePath() + File.separator + "logs"
						+ File.separator + testName + ".log");

				FileWriter f2 = null;
				int read = 0;
				try {
					f2 = new FileWriter(fnew, false);
					stdin.writeBytes("getevent\n");
					InputStream stdout = process.getInputStream();
					byte[] buffer = new byte[100];

					while (process.isAlive()) {
						read = stdout.read(buffer);
						String str = new String(buffer, 0, read);

						f2.write(str);
						if (logMvnTest)
							System.out.print(str);
//						Thread.sleep(10);
					}
				} catch (Exception e) {
//					e.printStackTrace();
					// exception due to the end of file => idk how to prevent it
				} finally {
					if (f2 != null)
						try {
							f2.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
				}
			}

		}.start();

		process.waitFor();

		return process.exitValue() != 0;
	}

	private static void generateDAO(String testName, File pml) {
		File testDir = new File(Paths.get(pml.getParentFile().getAbsolutePath()).resolve(testName).toString());
		if (!testDir.exists())
			testDir.mkdir();

		String[] args = new String[] { pml.getAbsolutePath(), testDir.getAbsolutePath() };
		Generate.main(args);
	}

	private static void pasteUnitTests(String testName, File pml) {
		File original = new File(
				Paths.get(pml.getParentFile().getAbsolutePath()).resolve(testName + "Tests.java").toString());
		File copied = new File(
				Paths.get(pml.getParentFile().getAbsolutePath()).resolve(testName).toString() + File.separator + "src"
						+ File.separator + "test" + File.separator + "java" + File.separator + testName + "Tests.java");

		if (!copied.getParentFile().getParentFile().exists())
			copied.getParentFile().getParentFile().mkdir();
		if (!copied.getParentFile().exists())
			copied.getParentFile().mkdir();

		try (InputStream in = new BufferedInputStream(new FileInputStream(original));
				OutputStream out = new BufferedOutputStream(new FileOutputStream(copied))) {

			byte[] buffer = new byte[1024];
			int lengthRead;
			while ((lengthRead = in.read(buffer)) > 0) {
				out.write(buffer, 0, lengthRead);
				out.flush();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void getPMLFiles() {
		File[] filesList = unitTestDir.listFiles();
		for (File f : filesList) {
			if (f.isFile()) {
				String fileName = f.getName();
				int index = fileName.lastIndexOf('.');
				if (index > 0) {
					String testName = fileName.substring(0, index);
					String extension = fileName.substring(index + 1);
					if (extension.toLowerCase().equals("pml")) {
						testFiles.put(testName, f);
//						System.out.println("PML detected: " + fileName);
					}
				}
			}
		}
	}

	private static String getDate() {
		Date d = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		return "[" + dateFormat.format(d) + "] ";
	}
}
