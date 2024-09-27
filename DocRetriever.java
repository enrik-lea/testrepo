import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import com.ibm.mm.sdk.common.DKConstant;
import com.ibm.mm.sdk.common.DKConstantICM;
import com.ibm.mm.sdk.common.DKDDO;
import com.ibm.mm.sdk.common.DKLobICM;
import com.ibm.mm.sdk.common.DKNVPair;
import com.ibm.mm.sdk.common.DKParts;
import com.ibm.mm.sdk.common.dkIterator;
import com.ibm.mm.sdk.common.dkResultSetCursor;
import com.ibm.mm.sdk.server.DKDatastoreICM;

/*
 * DocRetriever.java
 *
 * Description: Retrieve document information from the IBM Content Manager repository
 *
 * Input:       a properties file containing a set of parameters,
 *              including the name of an external file containing
 *              the ITEMIDs of the documents to retrieve
 *
 * Output:      formatted information of the documents
 *
 */

public class DocRetriever
{
	static Properties properties = null;

	static String username = "";
	static String servername = "";
	static String password = "";
	static String connectionString = "";
	static String maxResults = "0";
	static String filename = "";
	static Boolean download = false;

	static
	{
		try
		{
			properties = new Properties();
			properties.load(new FileInputStream("DocRetriever.properties"));
			username = properties.getProperty("username");
			servername = properties.getProperty("servername");
			password = properties.getProperty("password");
			connectionString = properties.getProperty("connectionString");
			maxResults = properties.getProperty("maxResults");
			download = Boolean.parseBoolean(properties.getProperty("download"));
			filename =  properties.getProperty("filename");
		}
		catch (Exception e)
		{
			System.out.println("init: " + e.getMessage());
			System.exit(1);
		}
	}

	/*
	 * printDocumentParts(DKDDO ddo, DKNVPair[] options)
	 *
	 * Description: print formatted output of the document parts
	 *
	 * Input:       the document ddo (must have been previously retrieved)
	 *              retrieve options as a DKNVPair array
	 *
	 * Output:      formatted information
	 */

	private static void printDocumentParts(DKDDO ddo, DKNVPair[] options)
	{
		String methodName = "printDocumentParts()";
		DKDatastoreICM dsICM = null;
		try
		{
			dsICM = (DKDatastoreICM) ddo.getDatastore();
			System.out.println(String.format("%20s   %s", "parts:", ""));
			short dataid = ddo.dataId(DKConstant.DK_CM_NAMESPACE_ATTR,DKConstant.DK_CM_DKPARTS);
			if (dataid != 0)
			{
				int p = 1;
				DKParts dkParts = (DKParts) ddo.getData(dataid);
				if (dkParts.cardinality() == 0)
				{
					System.out.println(String.format("%20s   %s", "", "no parts found"));
				}
				else
				{
					dkIterator iter = dkParts.createIterator();
					while (iter.more())
					{
						long beginRetrieve = System.currentTimeMillis();
						DKLobICM part = (DKLobICM) iter.next();
						System.out.println(String.format("%28s", "part [" + p + "/" + dkParts.cardinality() + "]"));
						//System.out.println("     part [" + p + "/" + dkParts.cardinality() + "]");
						System.out.println(String.format("%35s = [%s]", "PART_ID", part.getPidObject().getPrimaryId()));
						int rc = -1;
						try
						{
							part.retrieve(DKConstant.DK_CM_CONTENT_NO);
							System.out.println(String.format("%35s = [%s]", "MIMETYPE", part.getMimeType()));
							System.out.println(String.format("%35s = %s", "SIZE", part.getSize()));
							int semanticType = part.getSemanticType();
							switch (semanticType)
							{
							case DKConstantICM.DK_ICM_SEMANTIC_TYPE_BASE:
								System.out.println(String.format("%35s = [%s]", "SEMANTICTYPE", "ICMBASE (" + semanticType + ")"));
								break;
							case DKConstantICM.DK_ICM_SEMANTIC_TYPE_ANNOTATION:
								System.out.println(String.format("%35s = [%s]", "SEMANTICTYPE", "ICMANNOTATION (" + semanticType + ")"));
								break;
							default:
								System.out.println(String.format("%35s = [%s]", "SEMANTICTYPE", semanticType));
								break;
							}

							System.out.println(String.format("%35s = [%s]", "rmName", part.getRMName()));
							System.out.println(String.format("%35s = [%s]", "collName", part.getSMSCollName()));

							String[] urls = part.getContentURLs(DKConstantICM.DK_CM_RETRIEVE, DKConstantICM.DK_CM_VERSION_LATEST, -1, -1, DKConstantICM.DK_ICM_GETINITIALRMURL);
							for (int i=0; i<urls.length; i++)
							{
								System.out.println(String.format("%35s = [%s]", "URL", urls[i]));
							}

							if (download)
							{
								// long beginDownload = System.currentTimeMillis();
								part.retrieve(DKConstant.DK_CM_CONTENT_YES);
								part.getContentToClientFile(part.getPidObject().getPrimaryId(), DKConstant.DK_CM_XDO_FILE_OVERWRITE);
								// long endDownload = System.currentTimeMillis() - beginDownload;
								// System.out.println(String.format("%35s = %s ms", "download", endDownload));
							}

							rc = 0;
						}
						catch (Exception e)
						{
							System.out.println("Exception: ERROR(1) " + methodName + " " + e.getMessage());
						}
						finally
						{
							long endRetrieve = System.currentTimeMillis() - beginRetrieve;
							System.out.println(String.format("%35s = %s ms, %s %s", "total", endRetrieve, ddo.getPidObject().getPrimaryId(), part.getPidObject().getPrimaryId() + " " + rc));
						}
						p++;
					}
				}
			}
			else
			{
				System.out.println(String.format("%20s   %s", "", "no DKParts attribute found"));
			}
		}
		catch (Exception e)
		{
			System.out.println("Exception: ERROR(2) " + methodName + " " + e.getMessage());
		}
	}

	/*
	 * printDDO(DKDDO ddo)
	 *
	 * Description: print formatted output of the document metadata
	 *
	 * Input:       the document ddo (must have been previously retrieved)
	 *
	 * Output:      formatted information
	 */

	private static void printDDO(DKDDO ddo)
	{
		try
		{
			System.out.println(String.format("%35s = [%s]", "ITEMID", ddo.getPidObject().getPrimaryId()));
			for (short a = 1; a <= ddo.propertyCount(); a++)
			{
				if (
						!(ddo.getProperty(a) instanceof com.ibm.mm.sdk.common.DKChildCollection) &&
						!(ddo.getProperty(a) instanceof com.ibm.mm.sdk.common.DKParts) &&
						!(ddo.getProperty(a) instanceof com.ibm.mm.sdk.common.DKNVPair)
						)
				System.out.println(String.format("%35s = [%s]", ddo.getPropertyName(a), ddo.getProperty(a)));
			}

			for (short j = 1; j <= ddo.dataCount(); j++)
			{
				if (
						!(ddo.getData(j) instanceof com.ibm.mm.sdk.common.DKChildCollection) &&
						!(ddo.getData(j) instanceof com.ibm.mm.sdk.common.DKParts) &&
						!(ddo.getData(j) instanceof com.ibm.mm.sdk.common.DKNVPair)
						)
					System.out.println(String.format("%35s = [%s]", ddo.getDataName(j), ddo.getData(j)));
			}
		}
		catch (Exception e)
		{
			System.out.println("ERROR " + e.getMessage());
		}
	}

	public static void main(String[] args)
	{
		DKDatastoreICM dsICM = null;
		dkResultSetCursor cursor = null;
		DKDDO ddo = null;

		try
		{
			dsICM = new DKDatastoreICM();
			dsICM.connect(servername, username, password, connectionString);
			System.out.println("connected to server " + servername + " as " + username);

			DKNVPair options[] = new DKNVPair[3];
			options[0] = new DKNVPair(DKConstant.DK_CM_PARM_MAX_RESULTS, maxResults);
			options[1] = new DKNVPair(DKConstant.DK_CM_PARM_RETRIEVE,    new Integer(DKConstant.DK_CM_CONTENT_YES));
			options[2] = new DKNVPair(DKConstant.DK_CM_PARM_END,         null);

			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = "";
			String query = "";

			while ((line = br.readLine()) != null)
			{
				query = "/*[@ITEMID = \"" + line + "\"]";

				System.out.print("query: " + query);

				cursor = dsICM.execute(query, DKConstantICM.DK_CM_XQPE_QL_TYPE, options);

				int count = 0;
				int cardinality = cursor.cardinality();

				if (cardinality == 0)
				{
					System.out.println(" no documents found");
					continue;
				}
				else
					System.out.println();

				while((ddo = cursor.fetchNext()) != null)
				{
					System.out.println("item [" + ++count + "/" + cardinality + "]");

					try
					{
						printDDO(ddo);
						printDocumentParts(ddo, options);
					}
					catch (Exception e)
					{
						System.out.println("ERROR " + e.getMessage());
					}
				}
			}
		}
		catch (Exception e)
		{
			System.out.println("ERROR in main: " + e.getMessage());
		}
		finally
		{
			if (cursor != null) try { cursor.destroy(); } catch (Exception e){}

			if (dsICM != null)
			{
				if (dsICM.isConnected())
				{
					try
					{
						dsICM.disconnect();
						System.out.println("disconnected");
					}
					catch (Exception e){}
					try {dsICM.destroy();} catch (Exception e){}

				}
			}
		}
	}

}


