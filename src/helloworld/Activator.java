package helloworld;
import java.io.File;  
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;

import org.omg.CORBA.portable.InputStream;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
public class Activator implements BundleActivator {

	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#start(org.osgi.framework.BundleContext)
	 */
	private Process process;
	public void start(BundleContext context) throws Exception {
		System.out.println("Hello World!!");
		Bundle[] allbundles = context.getBundles();
		for(int i = 0; i < allbundles.length; i++)
			System.out.println(allbundles[i].getSymbolicName());
		File f = new File("/Users/huang/Downloads/data-integration");
		final String[] carte = {"/Users/huang/Downloads/data-integration/carte.sh","127.0.0.1","8080"};
		process = Runtime.getRuntime().exec(carte,null, f);
		java.io.InputStream is = process.getInputStream();   
         // 用一个读输出流类去读   
        InputStreamReader isr = new InputStreamReader(is);   
         // 用缓冲器读行   
        BufferedReader br = new BufferedReader(isr);   
        String line = null;   
        while ((line = br.readLine()) != null) {   
             System.out.println(line); 
        }
		System.out.println("2016/05/30 18:19:40 - Carte - Installing timer to purge stale objects after 1440 minutes.\n2016/05/30 18:19:40 - Carte - 创建 web 服务监听器 @ 地址: 192.168.142.128:8080");
		//System.out.println("nimahai");
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.osgi.framework.BundleActivator#stop(org.osgi.framework.BundleContext)
	 */
	public void stop(BundleContext context) throws Exception {
		//process.destroy();
		System.out.println("Goodbye World!!");
		
	}

}
