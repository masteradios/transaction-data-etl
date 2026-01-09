from dagster import ConfigurableResource
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from typing import List, Optional
import base64
from pathlib import Path


class EmailResource(ConfigurableResource):
    """
    Configurable Email Resource for sending emails with attachments
    """
    smtp_server: str
    smtp_port: int
    sender_email: str
    sender_password: str
    use_tls: bool = True
    use_ssl: bool = False
    
    def send(
        self,
        recipient_email: str,
        subject: str,
        template: str,
        attachments: Optional[List[dict]] = None
    ) -> dict:
        """
        Send email with optional attachments
        
        Args:
            recipient_email: Recipient's email address (can be comma-separated for multiple)
            subject: Email subject
            template: HTML or plain text email body
            attachments: List of dicts with format:
                [
                    {
                        'filename': 'report.pdf',
                        'content': base64_encoded_string,  # Base64 encoded file content
                        'mimetype': 'application/pdf'      # Optional, will be auto-detected
                    },
                    # OR
                    {
                        'filepath': '/path/to/file.pdf'    # Direct file path
                    }
                ]
        
        Returns:
            dict: Status of email sending
        """
        try:
            # Create message container
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = recipient_email
            msg['Subject'] = subject
            
            # Attach the email body
            # Detect if template is HTML or plain text
            if '<html>' in template.lower() or '<body>' in template.lower():
                msg.attach(MIMEText(template, 'html'))
            else:
                msg.attach(MIMEText(template, 'plain'))
            
            # Process attachments if provided
            if attachments:
                for attachment in attachments:
                    self._attach_file(msg, attachment)
            
            # Create SMTP session
            if self.use_ssl:
                # Use SMTP_SSL for port 465
                server = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            else:
                # Use regular SMTP with optional STARTTLS
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                if self.use_tls:
                    server.starttls()
            
            # Login and send email
            server.login(self.sender_email, self.sender_password)
            text = msg.as_string()
            server.sendmail(self.sender_email, recipient_email.split(','), text)
            server.quit()
            
            return {
                'status': 'success',
                'message': f'Email sent successfully to {recipient_email}',
                'recipient': recipient_email,
                'attachments_count': len(attachments) if attachments else 0
            }
            
        except Exception as e:
            return {
                'status': 'failed',
                'message': f'Failed to send email: {str(e)}',
                'recipient': recipient_email,
                'error': str(e)
            }
    
    def _attach_file(self, msg: MIMEMultipart, attachment: dict):
        """
        Attach a file to the email message
        
        Args:
            msg: MIMEMultipart message object
            attachment: Dict containing either base64 content or filepath
        """
        try:
            # Handle file path attachment
            if 'filepath' in attachment:
                filepath = attachment['filepath']
                filename = os.path.basename(filepath)
                
                with open(filepath, 'rb') as file:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(file.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', f'attachment; filename={filename}')
                    msg.attach(part)
            
            # Handle base64 encoded content
            elif 'content' in attachment and 'filename' in attachment:
                filename = attachment['filename']
                content_base64 = attachment['content']
                
                # Decode base64 content
                file_content = base64.b64decode(content_base64)
                
                # Determine MIME type
                mimetype = attachment.get('mimetype', 'application/octet-stream')
                maintype, subtype = mimetype.split('/', 1) if '/' in mimetype else ('application', 'octet-stream')
                
                part = MIMEBase(maintype, subtype)
                part.set_payload(file_content)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={filename}')
                msg.attach(part)
            
            else:
                raise ValueError("Attachment must contain either 'filepath' or both 'content' and 'filename'")
                
        except Exception as e:
            raise Exception(f"Failed to attach file: {str(e)}")
    
    def send_bulk(
        self,
        recipients: List[str],
        subject: str,
        template: str,
        attachments: Optional[List[dict]] = None
    ) -> List[dict]:
        """
        Send email to multiple recipients
        
        Args:
            recipients: List of email addresses
            subject: Email subject
            template: Email body template
            attachments: Optional list of attachments
        
        Returns:
            List of results for each recipient
        """
        results = []
        for recipient in recipients:
            result = self.send(recipient, subject, template, attachments)
            results.append(result)
        return results