package com.em.bdmp.common.kettle;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.util.zip.GZIPInputStream;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.methods.GetMethod;

import com.em.bdmp.framework.jobmanage.entity.JobServer;

/**
 * 
 * 对KettleHttpClient请求封装类
 * @author zhangtao
 *
 */
public class HttpKettle {
	
	/**
	 * Default we store our information in Unicode UTF-8 character set.
	 */
	public static final String XML_ENCODING = "UTF-8";
	
	public static final int ZIP_BUFFER_SIZE = 8192;
	
	/**
	 * runTrans
	 */
	public static final String CONTEXT_RUNTRANS = "/kettle/runTrans";
	
	/**
	 * 运行状态
	 */
	public static final String CONTEXT_TANSSTATUS = "/kettle/transStatus";
	
	/**
	 * remove trans
	 */
	public static final String CONTEXT_REMOVETRANS = "/kettle/removeTrans";
	
	/**
	 * runJob
	 */
	public static final String CONTEXT_RUNTJOB = "/kettle/runJob";
	
	/**
	 * jobStatus
	 */
	public static final String CONTEXT_JOBSTATUS = "/kettle/jobStatus";
	
	/**
	 * remove job
	 */
	public static final String CONTEXT_REMOVEJOB = "/kettle/removeJob";
	
	/**
	 * 
	 * @param service
	 * @param jobServer
	 * @return
	 * @throws Exception
	 */
	public static String execService(String service, JobServer jobServer){
		GetMethod method = new GetMethod( constructUrl(service, jobServer) );
		String responseBody = "";
		try{
			int result = HttpKettle.getHttpClient(jobServer).executeMethod(method);
			
			responseBody = HttpKettle.getResponseBodyAsString(method.getResponseBodyAsStream());
			if ( result >= 400 ) {
				throw new Exception( String.format( "HTTP Status %d - %s - %s", method.getStatusCode(), method.getPath(), method.getStatusText() ) );
			}
		}catch (Exception e) {
			responseBody = "Exception:" + e.getMessage();
		} finally{
			method.releaseConnection();
		}
		
		return responseBody;
	}

	/**
	 * httpClient
	 * @param jobServer
	 * @return
	 */
	private static HttpClient getHttpClient(JobServer jobServer) {
		MultiThreadedHttpConnectionManager manager = new MultiThreadedHttpConnectionManager();
		manager.getParams().setDefaultMaxConnectionsPerHost(100);
		manager.getParams().setMaxTotalConnections(200);
		HttpClient client = new HttpClient(manager);
		client.getState().setCredentials(
				new AuthScope(jobServer.getServIP(), Integer.parseInt(jobServer.getServPort())),
				new UsernamePasswordCredentials(jobServer.getServAccount(), jobServer.getServPwd()));
		client.getParams().setAuthenticationPreemptive(true);
		return client;
	}
	
	/**
	 * 获取responseBody内容
	 * @param is
	 * @return
	 * @throws IOException
	 */
	private static String getResponseBodyAsString(InputStream is) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
		StringBuilder bodyBuffer = new StringBuilder();
		String line;

		try {
			while ((line = bufferedReader.readLine()) != null) {
				bodyBuffer.append(line);
			}
		} finally {
			bufferedReader.close();
		}

		return bodyBuffer.toString();
	}
	
	/**
	 * 
	 * @param serviceAndArguments
	 * @param jobServer
	 * @return
	 */
	private static String constructUrl(String serviceAndArguments, JobServer jobServer){
		String realHostname = jobServer.getServIP();
		String retval = "http://" + realHostname + getPortSpecification(jobServer.getServPort()) + serviceAndArguments;
		retval = replace( retval, " ", "%20" );
		return retval;
	}
	
	/**
	 * 
	 * @param string
	 * @param repl
	 * @param with
	 * @return
	 */
	private static final String replace( String string, String repl, String with ) {
		StringBuffer str = new StringBuffer( string );
		for ( int i = str.length() - 1; i >= 0; i-- ) {
			if ( str.substring( i ).startsWith( repl ) ) {
				str.delete( i, i + repl.length() );
				str.insert( i, with );
			}
		}
		return str.toString();
	}
	
	
	/**
	 * 
	 * @param realPort
	 * @return
	 */
	private static String getPortSpecification(String realPort) {
		String portSpec = ":" + realPort;
		if ( realPort == null || realPort.equals( "80" ) ) {
			portSpec = "";
		}
		return portSpec;
	}

	public static String decodeBase64ZippedString( String loggingString64 ) throws IOException {
		if ( loggingString64 == null || loggingString64.isEmpty() ) {
			return "";
		}
		StringWriter writer = new StringWriter();
		// base 64 decode
		byte[] bytes64 = Base64.decodeBase64( loggingString64.getBytes() );
		// unzip to string encoding-wise
		ByteArrayInputStream zip = new ByteArrayInputStream( bytes64 );

		GZIPInputStream unzip = null;
		InputStreamReader reader = null;
		BufferedInputStream in = null;
		try {
			unzip = new GZIPInputStream( zip, HttpKettle.ZIP_BUFFER_SIZE );
			in = new BufferedInputStream( unzip, HttpKettle.ZIP_BUFFER_SIZE );
			// PDI-4325 originally used xml encoding in servlet
			reader = new InputStreamReader( in, HttpKettle.XML_ENCODING );
			writer = new StringWriter();

			// use same buffer size
			char[] buff = new char[HttpKettle.ZIP_BUFFER_SIZE];
			for ( int length = 0; ( length = reader.read( buff ) ) > 0; ) {
				writer.write( buff, 0, length );
			}
		} finally {
			// close resources
			if ( reader != null ) {
				try {
					reader.close();
				} catch ( IOException e ) {
					// Suppress
				}
			}
			if ( in != null ) {
				try {
					in.close();
				} catch ( IOException e ) {
					// Suppress
				}
			}
			if ( unzip != null ) {
				try {
					unzip.close();
				} catch ( IOException e ) {
					// Suppress
				}
			}
		}
		return writer.toString();
	}
}
