package com.hortonworks.nifi.custom.processor.standard;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Tags({ "ssh", "jsch", "remote", "execute", "command" })
@CapabilityDescription("Remote Login to a machine, executes the user specified command and returns the result as a flowfile. For commands that do not have a result to return, an empty flow file is generated. "
		+ "For any failure, the exception trace is available on the failure relationship")
@WritesAttributes({
    @WritesAttribute(attribute = "execution.time", description = "Time taken to execute the remote command")})
@InputRequirement(Requirement.INPUT_REQUIRED)
public class ExecuteRemoteCommand extends AbstractProcessor{

	public static final PropertyDescriptor REMOTE_HOST = new PropertyDescriptor.Builder().name("Remote Host")
			.description("Hostname or IP of the remote host").required(true).expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor USER = new PropertyDescriptor.Builder().name("Remote User")
			.description("Username to login to remote host").required(true).expressionLanguageSupported(true).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Remote User Password")
			.description("Login password for the remote user").required(true).expressionLanguageSupported(true).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor COMMAND = new PropertyDescriptor.Builder().name("Command")
			.description("Command to be executed on the remote shell").required(true).expressionLanguageSupported(true).sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	private static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("All successfully processed FlowFiles are routed to this relationship").build();
    private static final Relationship REL_FAILURE = new Relationship.Builder().name("failure").description("Unsuccessful operations will be transferred to the failure relationship.").build();
    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<Relationship>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));
    private List<PropertyDescriptor> descriptors;
	
	 @Override
		protected void init(final ProcessorInitializationContext context) {
			final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
			descriptors.add(REMOTE_HOST);
			descriptors.add(USER);
			descriptors.add(PASSWORD);
			descriptors.add(COMMAND);
			this.descriptors = Collections.unmodifiableList(descriptors);
		}
	    
	    @Override
		public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
			return descriptors;
		}
	    
	    @Override
	    public Set<Relationship> getRelationships() {
	        return RELATIONSHIPS;
	    }
	    
	    
	    private void submitFlowFile(FlowFile flowFile, final ProcessContext context, final ProcessSession session, final String message, final long startNanos, final String host_command, final Relationship rel){
		        flowFile = session.write(flowFile, new OutputStreamCallback() {
		            @Override
		            public void process(final OutputStream out) throws IOException {
		                out.write(message.getBytes(StandardCharsets.UTF_8));
		            }
		        });
		        final long transferMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
		        session.getProvenanceReporter().send(flowFile, host_command, transferMillis);
		        final Map<String, String> attributes = new HashMap<>();
		        attributes.put("execution.time", String.valueOf(transferMillis));
		        flowFile = session.putAllAttributes(flowFile, attributes);
		        session.transfer(flowFile, rel);
		        session.commit();
		}
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		
		boolean exception=false;
		JSch jschSSHChannel = new JSch();
		
		
		String userName = getProperty(context, USER);
		String hostName = getProperty(context, REMOTE_HOST);
		String password = getProperty(context, PASSWORD);
		String command = getProperty(context, COMMAND);
		String executionResult ="";

		 FlowFile flowFile = session.get();
	        if (flowFile == null) {
	            return;
	        }
	        
	        
		final long startNanos = System.nanoTime();
		try{
		Session remoteSession = jschSSHChannel.getSession(userName.trim(), hostName.trim());
		remoteSession.setPassword(password);
		remoteSession.setConfig("StrictHostKeyChecking", "no");
		remoteSession.connect();
		executionResult = executeCommand(command.trim(), remoteSession);
		remoteSession.disconnect();
		
		}catch(Exception e){
			exception=true;
			getLogger().error("Failed to execute remote command", new Object[] { command }, e);
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			executionResult=sw.toString();
			
		}
		if(!exception){
			submitFlowFile(flowFile,context, session, executionResult, startNanos, hostName+":"+command, REL_SUCCESS);
		}else{
			submitFlowFile(flowFile,context, session, executionResult, startNanos, hostName+":"+command, REL_FAILURE);
		}
		
	}
	
	  private String executeCommand(String command,Session session) throws Exception
	  {
	     StringBuilder outputBuffer = new StringBuilder();
	        Channel channel = session.openChannel("exec");
	        ((ChannelExec)channel).setCommand(command);
	        InputStream commandOutput = channel.getInputStream();
	        channel.connect();
	        int readByte = commandOutput.read();
	        while(readByte != 0xffffffff)
	        {
	           outputBuffer.append((char)readByte);
	           readByte = commandOutput.read();
	        }
	        channel.disconnect();
	     return outputBuffer.toString();
	  }
	  
	  public String getProperty(ProcessContext context, PropertyDescriptor descriptor) {
			if(descriptor.isExpressionLanguageSupported()){
				return context.getProperty(descriptor).evaluateAttributeExpressions().getValue().toString().trim();
			}else{
				return context.getProperty(descriptor).getValue().toString().trim();
			}
		}

}
