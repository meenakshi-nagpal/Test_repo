package jobs.util;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import models.storageapp.AppConfigProperty;
import play.Logger;
import utilities.Constant;

public class EmailSender {

	
	
	public static void sendEmail(String emailTo, String emailFrom, String emailSubject, String emailBody) throws Exception{  
		Logger.info("Entering sendEmail");

		
		String smtpServer = null; 	
		
		AppConfigProperty appConfigProperty = AppConfigProperty.getPropertyByKey(Constant.SMTP_SERVER);
   		if(appConfigProperty == null || appConfigProperty.getValue() == null || appConfigProperty.getValue().trim().isEmpty()) {
			Logger.warn("EmailSender - "+Constant.SMTP_SERVER + " not found, cannot send email");
			return;
		}else{
			smtpServer = appConfigProperty.getValue();
		}
		
		Properties props = new Properties();
		props.put("mail.smtp.host", smtpServer);
		props.put("mail.transport.protocol", "smtp");
		props.put("mail.debug", "true");
		props.put("mail.smtp.port", "25");
		
		Session session = Session.getInstance(props);
		try {

			Message message = new MimeMessage(session);
			message.setFrom(new InternetAddress(emailFrom));
			message.setRecipients(Message.RecipientType.TO,InternetAddress.parse(emailTo));
			message.setSubject(emailSubject);
			
			message.setContent(emailBody,"text/html");
			//message.setText(emailBody);

			Transport.send(message);

			Logger.info("email successfully sent");
			Logger.info("Exiting sendEmail");
		} catch (MessagingException e) {
			Logger.error("Error occurred while sending email : "+e);
			e.printStackTrace();
		}
 
		}
	
	}
