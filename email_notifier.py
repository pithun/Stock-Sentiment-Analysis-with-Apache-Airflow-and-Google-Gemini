"""
Module for sending email notifications with AI stock advice
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import markdown
from main import main

def format_file_to_html(file_path):
    """
    Converts a Markdown-formatted text file into a styled HTML email.

    Parameters:
        file_path (str): Path to the Markdown file.

    Returns:
        str: HTML content with inline CSS styling.
    """
    # Read the raw Markdown text from the file
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text = f.read()
    
    # Convert Markdown to HTML
    html_body = markdown.markdown(raw_text)
    
    # Wrap the HTML body in a complete HTML email template with inline CSS
    html_email = f"""
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>AI Generated Stock News & Investment Advice</title>
        <style>
          body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            margin: 0;
            padding: 0;
            background-color: #f9f9f9;
          }}
          .container {{
            max-width: 800px;
            margin: 30px auto;
            background-color: #fff;
            padding: 20px;
            border: 1px solid #ddd;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
          }}
          h1 {{
            color: #00539C;
          }}
          h2 {{
            color: #0073e6;
            border-bottom: 1px solid #ddd;
            padding-bottom: 5px;
          }}
          ul {{
            list-style-type: disc;
            margin-left: 20px;
          }}
          .good {{
            color: green;
            font-weight: bold;
          }}
          .bad {{
            color: red;
            font-weight: bold;
          }}
          .disclaimer {{
            font-size: 0.9em;
            color: #666;
            margin-top: 20px;
            border-top: 1px solid #ccc;
            padding-top: 10px;
          }}
        </style>
      </head>
      <body>
        <div class="container">
          <h2>AI Analysis on Stock News Provided</h2>
          {html_body}
        </div>
      </body>
    </html>
    """
    return html_email


def send_email_notification(smtp_url, smtp_user, smtp_password, to_emails, file_path, exec_date):
    """
    Formats the AI advice file as an HTML email and sends it to designated recipients.

    Parameters:
        smtp_url (str): SMTP server URL
        smtp_user (str): SMTP username
        smtp_password (str): SMTP password
        to_emails: comma sepparated email addresses
        file_path (str): Path to the AI advice file
        exec_date (str): Execution date for the subject line
    """
    subject = f"AI Analysis Report on Stock News Yesterday {exec_date}"
    html_content = format_file_to_html(file_path)

    # Parse email addresses (support both comma-separated string and list)
    if isinstance(to_emails, str):
        recipient_list = [email.strip() for email in to_emails.split(',')]
    else:
        recipient_list = to_emails
    
    # Create message
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] =  ', '.join(recipient_list)
    msg['Subject'] = subject
    
    # Attach HTML content
    msg.attach(MIMEText(html_content, 'html'))
    
    # Attach file
    with open(file_path, 'rb') as attachment:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename= {file_path.split("/")[-1]}')
        msg.attach(part)
    
    # Send email
    server = smtplib.SMTP(smtp_url, 587)
    server.starttls()
    server.login(smtp_user, smtp_password)
    text = msg.as_string()
    server.sendmail(smtp_user, recipient_list, text)  # Send to all recipients
    server.quit()
    
    print(f"Email sent successfully to {len(recipient_list)} recipient(s): {', '.join(recipient_list)}")

if __name__ == "__main__":
    functions = {'send_email_notification': send_email_notification}
    main(functions=functions, segment="email")