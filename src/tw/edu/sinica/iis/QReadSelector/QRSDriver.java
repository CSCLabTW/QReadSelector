package tw.edu.sinica.iis.QReadSelector;

import org.apache.hadoop.util.ProgramDriver;

public class QRSDriver {
	public static void main(String[] args) {
		ProgramDriver pgd = new ProgramDriver();

		int exitCode = -1;

		try {
			pgd.addClass("MinimalProductQ", MinimalProductQ.class, "");
			pgd.addClass("MinimalProductQsta", MinimalProductQsta.class, "");
			pgd.addClass("MinimalQFilter", MinimalQFilter.class, "");
			pgd.addClass("MinimalQ", MinimalQ.class, "");
			pgd.addClass("PEMinimalQFilter", PEMinimalQFilter.class, "");
			pgd.addClass("PEMQExtractor", PEMQExtractor.class, "");
			pgd.addClass("PQFilter", PQFilter.class, "");
			pgd.addClass("Qstatistics", Qstatistics.class, "");

			pgd.driver(args);

			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}

		System.exit(exitCode);
	}
}
