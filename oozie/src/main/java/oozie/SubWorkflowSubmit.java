package oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

public class SubWorkflowSubmit {
	
	private static OozieClient client = null;
	private static final String OOZIE_URL = "http://cm02:11000/oozie";
	
	public static void main(String[] args) throws Exception {
		
		init();
		
		test();
	}
	
	private static void test() throws Exception {
		Properties conf = client.createConfiguration();
		conf.setProperty("nameNode", "hdfs://cm02:8020");
		conf.setProperty("jobTracker", "cm02:8032");
//		conf.setProperty("queueName", "default");
//		conf.setProperty("examplesRoot", "examples");
//		conf.setProperty("outputDir_1", "subwf_1");
//		conf.setProperty("outputDir_2", "subwf_2");
		conf.setProperty(OozieClient.USER_NAME, "yezhimin");
		conf.setProperty(OozieClient.APP_PATH, "${nameNode}/user/hue/oozie/workspaces/hue-oozie-1550661294.12");
//		conf.setProperty(OozieClient.LIBPATH, "/user/${user.name}/${examplesRoot}/lib");
		conf.setProperty("oozie.use.system.libpath", "true");
		
		String jobId = client.run(conf);
		System.out.println("job submit finished. jobId:" + jobId);
		
	    while (getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
	        System.out.println("Workflow job running ...");
	        Thread.sleep(10 * 1000);
	    }
	    
	    System.out.println("Workflow job completed ...");
	    System.out.println("Workflow job final status:" + getJobInfo(jobId).getStatus());
	}
	
	private static WorkflowJob getJobInfo(String jobId) throws OozieClientException {
		return client.getJobInfo(jobId);
	}

	private static void init() {
		client = new OozieClient(OOZIE_URL);
	}
	
	
}
